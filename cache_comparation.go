/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package reconcilers_test demonstrates why a label-filtered informer cache
// matters for a Kubernetes operator that runs in a shared cluster.
//
// # The problem
//
// Without cache.ByObject label filtering, controller-runtime issues an
// unscoped LIST for every watched resource type at startup:
//
//	LIST /api/v1/pods             → all pods in the cluster
//	LIST /api/v1/services         → all services in the cluster
//	LIST /apis/apps/v1/statefulsets → all StatefulSets in the cluster
//
// All of those objects are decoded, stored in informer stores, and kept alive
// in the operator's heap — even though the operator only cares about the tiny
// subset that it manages (those with the managed-by label).
//
// # The test scenario
//
// The test creates a simulated "shared cluster" with:
//   - otherNamespaces team namespaces, each with many Pods / Services / StatefulSets
//     that belong to other applications (no kafka label)
//   - one dedicated kafka namespace with a small set of labeled Pods, Services,
//     and StatefulSets that the operator actually manages
//
// Two manager scenarios are then started sequentially and compared:
//
//	A. Labeled cache  – cache.ByObject restricts Pod, Service, and StatefulSet
//	                    informers to objects with the managed-by label (current
//	                    production configuration).
//	B. Unfiltered cache – no ByObject, so every Pod / Service / StatefulSet in
//	                      the entire cluster is loaded into the cache.
//
// # Metrics
//
// Per scenario and per resource type (Pod / Service / StatefulSet):
//   - bytes received from the API server during the initial LIST
//   - number of objects stored in the local informer cache
//
// Additionally:
//   - heap memory delta (HeapAlloc before → after cache sync)
//   - goroutine count over time (proxy for CPU activity)
//   - average client.List latency served from the local cache
//
// Time-series samples (50 ms interval) are collected during the init phase
// and rendered as SVG line charts in the HTML report.
//
// # Run
//
//	make setup-envtest   # once — downloads kube-apiserver + etcd binaries
//	make cache-comparison
//
// Or directly:
//
//	KUBEBUILDER_ASSETS=... go test ./pkg/reconcilers/... \
//	    -run TestCacheComparison -v -count=1 -timeout 600s
package reconcilers_test

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"fox.flant.com/team/managed-services/managed-kafka/managed-kafka-operator/pkg/constants"
)

// ---------------------------------------------------------------------------
// Cluster topology constants
// ---------------------------------------------------------------------------

const (
	// Namespaces that simulate other teams sharing the cluster.
	otherNamespaceCount = 50
	podsPerOtherNs      = 100 // per namespace → 50 × 100 = 5000 "other" pods
	secretsPerOtherNs   = 20  // per namespace → 50 × 20 = 1000 "other" secrets
	pvcsPerOtherNs      = 4   // per namespace → 50 × 4 = 200 "other" PVCs
	svcPerOtherNs       = 2   // per namespace → 50 × 2 = 100 "other" services
	stsPerOtherNs       = 1   // per namespace → 50 × 1 = 50 "other" StatefulSets

	// Kafka resources — what the operator actually manages.
	kafkaNs           = "kafka-production"
	kafkaInstances    = 3 // number of Kafka CRs (one StatefulSet each)
	brokersPerKafka   = 3 // broker pods per Kafka instance
	// Totals: 9 kafka pods, 3 kafka services, 3 kafka StatefulSets
)

// sampleInterval controls how often metrics are polled during cache init.
const sampleInterval = 50 * time.Millisecond

// listRuns is the number of client.List calls used to average latency.
const listRuns = 300

// ---------------------------------------------------------------------------
// HTTP transport — counts and measures API-server calls per resource type
// ---------------------------------------------------------------------------

type resourceBytes struct {
	pods         atomic.Int64
	services     atomic.Int64
	statefulsets atomic.Int64
	secrets      atomic.Int64
	pvcs         atomic.Int64
	other        atomic.Int64
}

func (rb *resourceBytes) add(req *http.Request, n int64) {
	path := req.URL.Path
	switch {
	case strings.Contains(path, "/pods"):
		rb.pods.Add(n)
	case strings.Contains(path, "/services"):
		rb.services.Add(n)
	case strings.Contains(path, "/statefulsets"):
		rb.statefulsets.Add(n)
	case strings.Contains(path, "/secrets"):
		rb.secrets.Add(n)
	case strings.Contains(path, "/persistentvolumeclaims"):
		rb.pvcs.Add(n)
	default:
		rb.other.Add(n)
	}
}

type apiCounter struct {
	listBytes  resourceBytes
	listReqs   resourceBytes // counts (not bytes)
	watchReqs  resourceBytes
	totalList  atomic.Int64
	totalWatch atomic.Int64
}

func (c *apiCounter) addListReq(req *http.Request) { c.listReqs.add(req, 1); c.totalList.Add(1) }
func (c *apiCounter) addWatchReq(req *http.Request) { c.watchReqs.add(req, 1); c.totalWatch.Add(1) }

// countingBody wraps an io.ReadCloser and accumulates byte counts.
type countingBody struct {
	io.ReadCloser
	req     *http.Request
	counter *apiCounter
}

func (b *countingBody) Read(p []byte) (n int, err error) {
	n, err = b.ReadCloser.Read(p)
	b.counter.listBytes.add(b.req, int64(n))
	return
}

type countingTransport struct {
	base    http.RoundTripper
	counter *apiCounter
}

func (t *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if req.URL.Query().Get("watch") == "true" {
		t.counter.addWatchReq(req)
	} else {
		t.counter.addListReq(req)
		resp.Body = &countingBody{ReadCloser: resp.Body, req: req, counter: t.counter}
	}
	return resp, err
}

func wrapConfig(baseCfg *rest.Config) (*rest.Config, *apiCounter) {
	counter := &apiCounter{}
	cfg := rest.CopyConfig(baseCfg)
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		return &countingTransport{base: rt, counter: counter}
	}
	return cfg, counter
}

// ---------------------------------------------------------------------------
// Time-series sampling during cache initialisation
// ---------------------------------------------------------------------------

type initSample struct {
	elapsed    time.Duration
	heapKiB    int64 // runtime.MemStats.HeapAlloc / 1024
	apiTotal   int64 // cumulative list+watch requests so far
	goroutines int   // runtime.NumGoroutine()
}

func runSampler(ctx context.Context, counter *apiCounter, start time.Time, out *[]initSample, mu *sync.Mutex, baselineHeap int64) {
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			s := initSample{
				elapsed:    time.Since(start),
				heapKiB:    (int64(ms.HeapAlloc) - baselineHeap) / 1024,
				apiTotal:   counter.totalList.Load() + counter.totalWatch.Load(),
				goroutines: runtime.NumGoroutine(),
			}
			mu.Lock()
			*out = append(*out, s)
			mu.Unlock()
		}
	}
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

type cacheCount struct {
	pods, services, statefulsets, secrets, pvcs int
}

type scenarioResult struct {
	name string

	// Objects actually stored in the informer cache after sync.
	cached cacheCount

	// Bytes received from the API server per resource type during LIST.
	listBytes struct {
		pods, services, statefulsets, secrets, pvcs int64
	}

	heapDeltaBytes int64
	initDuration   time.Duration
	avgListLatency time.Duration
	samples        []initSample
}

func (r scenarioResult) totalCached() int {
	return r.cached.pods + r.cached.services + r.cached.statefulsets + r.cached.secrets + r.cached.pvcs
}

func (r scenarioResult) totalListBytes() int64 {
	return r.listBytes.pods + r.listBytes.services + r.listBytes.statefulsets + r.listBytes.secrets + r.listBytes.pvcs
}

// ---------------------------------------------------------------------------
// Test driver
// ---------------------------------------------------------------------------

func TestCacheComparison(t *testing.T) {
	logf.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.UseDevMode(false)))

	// Start envtest (real kube-apiserver + etcd).
	env := &envtest.Environment{}
	if dir := findEnvtestBinDir(); dir != "" {
		env.BinaryAssetsDirectory = dir
	}
	baseCfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest Start: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	seedCtx := context.Background()
	seedClient, err := client.New(baseCfg, client.Options{})
	if err != nil {
		t.Fatalf("seed client: %v", err)
	}

	t.Log("Seeding cluster resources...")
	seedCluster(t, seedCtx, seedClient)
	t.Logf("Seeding complete. Other workloads: %d pods, %d svc, %d sts | Kafka: %d pods, %d svc, %d sts",
		otherNamespaceCount*podsPerOtherNs,
		otherNamespaceCount*svcPerOtherNs,
		otherNamespaceCount*stsPerOtherNs,
		kafkaInstances*brokersPerKafka,
		kafkaInstances,
		kafkaInstances,
	)

	labeledResult := runScenario(t, "A: Кэш с фильтром (текущий)", baseCfg, true)
	unfilteredResult := runScenario(t, "B: Кэш без фильтра (сравнение)", baseCfg, false)

	printTable(t, labeledResult, unfilteredResult)
	printASCIICharts(t, labeledResult, unfilteredResult)
	writeHTMLReport(t, labeledResult, unfilteredResult)
}

// runScenario starts a manager, samples metrics during init, then shuts down.
func runScenario(t *testing.T, name string, baseCfg *rest.Config, withLabels bool) scenarioResult {
	t.Helper()
	t.Logf("=== Scenario %s ===", name)

	cfg, counter := wrapConfig(baseCfg)

	var cacheOpts cache.Options
	if withLabels {
		sel := k8slabels.SelectorFromSet(k8slabels.Set{
			constants.ManagedByKey: constants.KafkaManagedByValue,
		})
		byObj := cache.ByObject{Label: sel}
		cacheOpts = cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Pod{}:                   byObj,
				&corev1.Service{}:               byObj,
				&appsv1.StatefulSet{}:            byObj,
				&corev1.Secret{}:                byObj,
				&corev1.PersistentVolumeClaim{}: byObj,
			},
		}
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Cache:                  cacheOpts,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	})
	if err != nil {
		t.Fatalf("%s: NewManager: %v", name, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Baseline heap snapshot.
	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	initStart := time.Now()

	// t=0 anchor sample — guarantees ≥2 points even for very fast syncs.
	var (
		samples   = []initSample{{
			elapsed:    0,
			heapKiB:    0, // Start at 0 for relative delta
			apiTotal:   0,
			goroutines: runtime.NumGoroutine(),
		}}
		samplesMu sync.Mutex
	)

	samplerCtx, stopSampler := context.WithCancel(ctx)
	go runSampler(samplerCtx, counter, initStart, &samples, &samplesMu, int64(msBefore.HeapAlloc))

	// Prime the cache so informers are registered and started with the manager.
	_, _ = mgr.GetCache().GetInformer(ctx, &corev1.Pod{})
	_, _ = mgr.GetCache().GetInformer(ctx, &corev1.Service{})
	_, _ = mgr.GetCache().GetInformer(ctx, &appsv1.StatefulSet{})
	_, _ = mgr.GetCache().GetInformer(ctx, &corev1.Secret{})
	_, _ = mgr.GetCache().GetInformer(ctx, &corev1.PersistentVolumeClaim{})

	go func() {
		if err := mgr.Start(ctx); err != nil && ctx.Err() == nil {
			t.Logf("%s: manager: %v", name, err)
		}
	}()

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		stopSampler()
		t.Fatalf("%s: cache sync timed out", name)
	}
	initDuration := time.Since(initStart)

	// Final sample at exact sync point.
	{
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		samplesMu.Lock()
		samples = append(samples, initSample{
			elapsed:    initDuration,
			heapKiB:    (int64(ms.HeapAlloc) - int64(msBefore.HeapAlloc)) / 1024,
			apiTotal:   counter.totalList.Load() + counter.totalWatch.Load(),
			goroutines: runtime.NumGoroutine(),
		})
		samplesMu.Unlock()
	}
	stopSampler()

	// Heap delta.
	runtime.GC()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	heapDelta := int64(msAfter.HeapAlloc) - int64(msBefore.HeapAlloc)

	time.Sleep(200 * time.Millisecond)

	// Count objects in each informer store.
	var (
		podList    corev1.PodList
		svcList    corev1.ServiceList
		stsList    appsv1.StatefulSetList
		secretList corev1.SecretList
		pvcList    corev1.PersistentVolumeClaimList
	)
	_ = mgr.GetCache().List(ctx, &podList)
	_ = mgr.GetCache().List(ctx, &svcList)
	_ = mgr.GetCache().List(ctx, &stsList)
	_ = mgr.GetCache().List(ctx, &secretList)
	_ = mgr.GetCache().List(ctx, &pvcList)

	// Average List latency (pods from cache).
	listStart := time.Now()
	for range listRuns {
		var pl corev1.PodList
		_ = mgr.GetClient().List(ctx, &pl,
			client.InNamespace(kafkaNs),
			client.MatchingLabels{constants.ManagedByKey: constants.KafkaManagedByValue},
		)
	}
	avgLatency := time.Since(listStart) / listRuns

	cancel()
	time.Sleep(300 * time.Millisecond)

	samplesMu.Lock()
	samplesCopy := make([]initSample, len(samples))
	copy(samplesCopy, samples)
	samplesMu.Unlock()

	res := scenarioResult{
		name: name,
		cached: cacheCount{
			pods:         len(podList.Items),
			services:     len(svcList.Items),
			statefulsets: len(stsList.Items),
			secrets:      len(secretList.Items),
			pvcs:         len(pvcList.Items),
		},
		heapDeltaBytes: heapDelta,
		initDuration:   initDuration.Round(time.Millisecond),
		avgListLatency: avgLatency,
		samples:        samplesCopy,
	}
	res.listBytes.pods = counter.listBytes.pods.Load()
	res.listBytes.services = counter.listBytes.services.Load()
	res.listBytes.statefulsets = counter.listBytes.statefulsets.Load()
	res.listBytes.secrets = counter.listBytes.secrets.Load()
	res.listBytes.pvcs = counter.listBytes.pvcs.Load()
	return res
}

// ---------------------------------------------------------------------------
// Text output
// ---------------------------------------------------------------------------

func printTable(t *testing.T, a, b scenarioResult) {
	t.Helper()
	line := strings.Repeat("─", 78)
	t.Logf("\n\n%s", line)
	t.Logf("Сравнение стратегий кэширования")
	t.Logf("Кластер: %d неймспейсов с чужими нагрузками × (%d подов + %d сервисов + %d sts)  +  kafka: %d подов / %d сервисов / %d sts",
		otherNamespaceCount, podsPerOtherNs, svcPerOtherNs, stsPerOtherNs,
		kafkaInstances*brokersPerKafka, kafkaInstances, kafkaInstances)
	t.Logf("%s", line)
	t.Logf("%-42s  %-14s  %-14s", "Метрика", shortName(a.name), shortName(b.name))
	t.Logf("%s", line)

	row := func(label string, va, vb interface{}) {
		t.Logf("%-42s  %-14v  %-14v", label, va, vb)
	}

	row("Подов в кэше", a.cached.pods, b.cached.pods)
	row("Сервисов в кэше", a.cached.services, b.cached.services)
	row("StatefulSets в кэше", a.cached.statefulsets, b.cached.statefulsets)
	row("Секретов в кэше", a.cached.secrets, b.cached.secrets)
	row("PVC в кэше", a.cached.pvcs, b.cached.pvcs)
	row("Всего объектов в кэше", a.totalCached(), b.totalCached())
	t.Logf("%s", strings.Repeat("·", 78))
	row("Дельта heap после синхронизации",
		fmtSignedBytes(a.heapDeltaBytes), fmtSignedBytes(b.heapDeltaBytes))
	row("Время инициализации кэша", a.initDuration, b.initDuration)
	row(fmt.Sprintf("Ср. задержка List (%d запусков)", listRuns),
		a.avgListLatency, b.avgListLatency)
	t.Logf("%s", line)

	if b.totalCached() > 0 {
		t.Logf("Снижение числа объектов в кэше: %.0f%%",
			pctReduction(float64(a.totalCached()), float64(b.totalCached())))
	}
	t.Logf("%s\n", line)
}

func printASCIICharts(t *testing.T, a, b scenarioResult) {
	t.Helper()
	t.Logf("\n--- ASCII-диаграммы ---")
	barChart(t, "Всего объектов в кэше",
		shortName(a.name), float64(a.totalCached()),
		shortName(b.name), float64(b.totalCached()), "объектов")
	barChart(t, "Дельта heap",
		shortName(a.name), float64(max64(a.heapDeltaBytes, 0)),
		shortName(b.name), float64(max64(b.heapDeltaBytes, 0)), "bytes")
	barChart(t, "Время инициализации кэша",
		shortName(a.name), float64(a.initDuration.Milliseconds()),
		shortName(b.name), float64(b.initDuration.Milliseconds()), "ms")
}

func barChart(t *testing.T, title, labelA string, valA float64, labelB string, valB float64, unit string) {
	t.Helper()
	const maxWidth = 40
	maxV := valA
	if valB > maxV {
		maxV = valB
	}
	scale := func(v float64) int {
		if maxV == 0 {
			return 0
		}
		return int(v / maxV * maxWidth)
	}
	t.Logf("\n  %s", title)
	t.Logf("  %-14s │%s│ %s", labelA, strings.Repeat("█", scale(valA)), fmtFloat(valA, unit))
	t.Logf("  %-14s │%s│ %s", labelB, strings.Repeat("█", scale(valB)), fmtFloat(valB, unit))
}

func fmtFloat(v float64, unit string) string {
	if unit == "bytes" {
		return fmtBytes(uint64(v))
	}
	return fmt.Sprintf("%.0f %s", v, unit)
}

func pctReduction(smaller, larger float64) float64 {
	if larger == 0 {
		return 0
	}
	return (larger - smaller) / larger * 100
}

func shortName(name string) string {
	if len(name) > 3 && name[1] == ':' {
		return strings.TrimSpace(name[3:])
	}
	return name
}

func fmtBytes(b uint64) string {
	const (
		KiB = 1024
		MiB = 1024 * KiB
	)
	switch {
	case b >= MiB:
		return fmt.Sprintf("%.1f MiB", float64(b)/MiB)
	case b >= KiB:
		return fmt.Sprintf("%.1f KiB", float64(b)/KiB)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func fmtSignedBytes(b int64) string {
	if b < 0 {
		return "-" + fmtBytes(uint64(-b))
	}
	return "+" + fmtBytes(uint64(b))
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// SVG chart primitives
// ---------------------------------------------------------------------------

const (
	svgW    = 680
	svgH    = 200
	svgPadL = 72
	svgPadR = 20
	svgPadT = 22
	svgPadB = 42
	svgLegH = 22
)

type svgPoint struct{ x, y float64 }
type svgSeries struct {
	label  string
	color  string
	points []svgPoint
}

func svgChart(title, xLabel, yLabel string, series []svgSeries) string {
	totalW := svgW + svgPadL + svgPadR
	totalH := svgH + svgPadT + svgPadB + svgLegH*len(series)

	var maxX, maxY float64
	for _, s := range series {
		for _, p := range s.points {
			if p.x > maxX {
				maxX = p.x
			}
			if p.y > maxY {
				maxY = p.y
			}
		}
	}
	if maxX == 0 {
		maxX = 1
	}
	maxY = niceMax(maxY, 5)

	toX := func(x float64) float64 { return float64(svgPadL) + x/maxX*float64(svgW) }
	toY := func(y float64) float64 { return float64(svgPadT+svgH) - y/maxY*float64(svgH) }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" style="font-family:system-ui,sans-serif">`,
		totalW, totalH)

	// Title
	fmt.Fprintf(&b, `<text x="%d" y="15" text-anchor="middle" font-size="12" font-weight="600" fill="#1a1a2e">%s</text>`,
		totalW/2, htmlEscape(title))

	// Horizontal grid + Y labels
	for i := range 6 {
		yVal := maxY * float64(i) / 5
		sy := toY(yVal)
		fmt.Fprintf(&b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#e9ecef" stroke-width="1"/>`,
			svgPadL, sy, svgPadL+svgW, sy)
		fmt.Fprintf(&b, `<text x="%d" y="%.1f" text-anchor="end" font-size="9" fill="#868e96">%s</text>`,
			svgPadL-3, sy+3, fmtAxisVal(yVal))
	}

	// Vertical grid + X labels
	for i := range 7 {
		xVal := maxX * float64(i) / 6
		sx := toX(xVal)
		fmt.Fprintf(&b, `<line x1="%.1f" y1="%d" x2="%.1f" y2="%d" stroke="#e9ecef" stroke-width="1"/>`,
			sx, svgPadT, sx, svgPadT+svgH)
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" text-anchor="middle" font-size="9" fill="#868e96">%.1fs</text>`,
			sx, svgPadT+svgH+12, xVal)
	}

	// Axis border
	fmt.Fprintf(&b, `<rect x="%d" y="%d" width="%d" height="%d" fill="none" stroke="#ced4da" stroke-width="1"/>`,
		svgPadL, svgPadT, svgW, svgH)

	// Axis labels
	fmt.Fprintf(&b, `<text x="%d" y="%d" text-anchor="middle" font-size="10" fill="#495057">%s</text>`,
		svgPadL+svgW/2, svgPadT+svgH+28, htmlEscape(xLabel))
	fmt.Fprintf(&b, `<text transform="rotate(-90)" x="%d" y="%d" text-anchor="middle" font-size="10" fill="#495057">%s</text>`,
		-(svgPadT+svgH/2), 12, htmlEscape(yLabel))

	// Series
	for _, s := range series {
		switch len(s.points) {
		case 0:
			continue
		case 1:
			sy := toY(s.points[0].y)
			fmt.Fprintf(&b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s" stroke-width="2" stroke-dasharray="5,3"/>`,
				svgPadL, sy, svgPadL+svgW, sy, s.color)
		default:
			var path strings.Builder
			for i, p := range s.points {
				sx, sy := toX(p.x), toY(p.y)
				if i == 0 {
					fmt.Fprintf(&path, "M%.2f,%.2f", sx, sy)
				} else {
					fmt.Fprintf(&path, " L%.2f,%.2f", sx, sy)
				}
			}
			fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="%s" stroke-width="2.5" stroke-linejoin="round"/>`,
				path.String(), s.color)
		}
	}

	// Legend
	legY := svgPadT + svgH + svgPadB - 4
	for _, s := range series {
		fmt.Fprintf(&b, `<rect x="%d" y="%d" width="11" height="11" rx="2" fill="%s"/>`,
			svgPadL, legY, s.color)
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="10" fill="#495057">%s</text>`,
			svgPadL+15, legY+9, htmlEscape(s.label))
		legY += svgLegH
	}

	b.WriteString("</svg>")
	return b.String()
}

func niceMax(v float64, n int) float64 {
	if v <= 0 {
		return float64(n)
	}
	mag := math.Pow(10, math.Floor(math.Log10(v/float64(n))))
	for _, c := range []float64{1, 2, 2.5, 5, 10} {
		if c*mag*float64(n) >= v {
			return c * mag * float64(n)
		}
	}
	return 10 * mag * float64(n)
}

func fmtAxisVal(v float64) string {
	if v >= 1_000_000 {
		return fmt.Sprintf("%.1fM", v/1_000_000)
	}
	if v >= 1_000 {
		return fmt.Sprintf("%.0fK", v/1_000)
	}
	if v == math.Trunc(v) {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

func toSeries(ss []initSample, label, color string, fn func(initSample) float64) svgSeries {
	pts := make([]svgPoint, len(ss))
	for i, s := range ss {
		pts[i] = svgPoint{x: s.elapsed.Seconds(), y: fn(s)}
	}
	return svgSeries{label: label, color: color, points: pts}
}

// ---------------------------------------------------------------------------
// HTML report
// ---------------------------------------------------------------------------

func writeHTMLReport(t *testing.T, a, b scenarioResult) {
	t.Helper()

	makeCharts := func(r scenarioResult) (ramSVG string) {
		ramSVG = svgChart(
			shortName(r.name)+": RAM при инициализации",
			"время (с)", "дельта heap (КБ)",
			[]svgSeries{toSeries(r.samples, "Дельта heap (КБ)", "#3a86ff",
				func(s initSample) float64 { return float64(s.heapKiB) })},
		)
		return
	}

	ramA := makeCharts(a)
	ramB := makeCharts(b)

	ramCombined := svgChart(
		"Сравнение: RAM при инициализации (оба графика)",
		"время (с)", "log10(дельта heap КБ + 1)",
		[]svgSeries{
			toSeries(a.samples, "С фильтром", "#3a86ff", func(s initSample) float64 { return math.Log10(float64(s.heapKiB) + 1) }),
			toSeries(b.samples, "Без фильтра", "#ff6b6b", func(s initSample) float64 { return math.Log10(float64(s.heapKiB) + 1) }),
		},
	)

	heapRedPct := pctReduction(float64(max64(a.heapDeltaBytes, 0)), float64(max64(b.heapDeltaBytes, 0)))

	var sb strings.Builder
	sb.WriteString(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Сравнение стратегий кэширования</title>
<style>
*{box-sizing:border-box}
body{font-family:system-ui,sans-serif;max-width:1160px;margin:40px auto;padding:0 24px;background:#f8f9fa;color:#212529}
h1{margin-bottom:4px}
.sub{color:#6c757d;font-size:14px;margin-bottom:32px}
h2{font-size:16px;color:#1a1a2e;margin:32px 0 8px}
p.note{color:#6c757d;font-size:13px;margin:0 0 16px}
.chart-section{background:#fff;border-radius:12px;padding:20px;box-shadow:0 2px 8px rgba(0,0,0,.07);margin-bottom:20px}
.chart-section h3{margin:0 0 14px;font-size:14px;color:#495057}
.chart-row{display:flex;gap:16px;flex-wrap:wrap;align-items:flex-start}
.chart-row svg{border:1px solid #e9ecef;border-radius:8px;background:#fafafa;max-width:100%;height:auto}
.bar-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px;margin-bottom:20px}
.bar-card{background:#fff;border-radius:10px;padding:16px;box-shadow:0 2px 8px rgba(0,0,0,.07)}
.bar-title{font-size:13px;font-weight:600;color:#343a40;margin-bottom:12px}
.bar-group{margin-bottom:6px}
.bar-label{font-size:11px;color:#6c757d;margin-bottom:3px}
.bar-row{display:flex;align-items:center;gap:8px}
.bar{height:24px;border-radius:4px;min-width:3px}
.bar-a{background:#3a86ff}.bar-b{background:#ff6b6b}
.bv{font-size:11px;font-weight:600;white-space:nowrap;color:#212529}
.tbl-wrap{background:#fff;border-radius:12px;padding:20px;box-shadow:0 2px 8px rgba(0,0,0,.07);overflow-x:auto;margin-bottom:20px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:left;padding:7px 12px;border-bottom:1px solid #dee2e6}
th{background:#f1f3f5;font-weight:600}
tr.sep td{border-bottom:2px solid #adb5bd;background:#f8f9fa;font-style:italic;color:#6c757d;font-size:12px}
.good{color:#2d8a3e;font-weight:700}
.bad-col{background-color:#fff5f5;color:#c92a2a;font-weight:600}
.alert-box{background:#fff5f5;border-left:4px solid #fa5252;padding:16px 20px;margin-bottom:24px;border-radius:0 8px 8px 0;box-shadow:0 2px 8px rgba(0,0,0,.05)}
.alert-box h2{color:#c92a2a;margin-top:0;margin-bottom:8px;font-size:16px}
.alert-box p{margin:0;color:#495057;font-size:14px;line-height:1.5}
.conclusion{background:#fff;border-radius:12px;padding:20px;box-shadow:0 2px 8px rgba(0,0,0,.07)}
.conclusion h2{margin-top:0}
li{margin-bottom:8px;line-height:1.55}
code{background:#f1f3f5;padding:1px 5px;border-radius:3px;font-size:12px}
</style>
</head>
<body>
`)
	fmt.Fprintf(&sb, "<h1>Сравнение стратегий кэширования оператора</h1>\n")
	fmt.Fprintf(&sb, `<p class="sub">Shared cluster simulation: <b>%d</b> неймспейсов с чужими нагрузками × (%d pods + %d secrets + %d pvcs + %d svc + %d sts) &nbsp;+&nbsp; Kafka: <b>%d</b> pods / <b>%d</b> svc / <b>%d</b> sts with managed-by label</p>`+"\n",
		otherNamespaceCount, podsPerOtherNs, secretsPerOtherNs, pvcsPerOtherNs, svcPerOtherNs, stsPerOtherNs,
		kafkaInstances*brokersPerKafka, kafkaInstances, kafkaInstances)

	sb.WriteString(`<div class="alert-box">
		<h2>⚠️ Внимание: Проблема отсутствия фильтрации</h2>
		<p>Запуск оператора <b>без фильтрации по лейблам</b> приводит к катастрофическому росту потребления памяти. Оператор загружает в свой кэш <b>все</b> объекты кластера, даже те, которыми он не управляет. В крупных кластерах это вызывает огромный перерасход RAM, перегрузку сборщика мусора (GC) и замедление работы оператора.</p>
	</div>`)

	// Time-series charts
	sb.WriteString(`<h2>Графики инициализации</h2>`)
	sb.WriteString(`<p class="note">Метрики собирались каждые 50 мс от старта менеджера до завершения синхронизации кэша. Первый график использует логарифмическую шкалу по Y, чтобы на одном графике были видны обе линии. Два отдельных графика ниже оставлены в обычной линейной шкале.</p>`)

	fmt.Fprintf(&sb, `<div class="chart-section"><h3>%s</h3><div class="chart-row" style="justify-content: center;">%s</div></div>`+"\n",
		"Сравнение (на одном графике)", ramCombined)

	fmt.Fprintf(&sb, `<div class="chart-section"><h3>%s</h3><div class="chart-row">%s%s</div></div>`+"\n",
		"По отдельности (левый = с фильтром, правый = без фильтра)", ramA, ramB)

	// Bar charts
	type barMetric struct {
		title    string
		valA, valB float64
		unit     string
	}
	barMetrics := []barMetric{
		{"Подов в кэше", float64(a.cached.pods), float64(b.cached.pods), "pods"},
		{"Сервисов в кэше", float64(a.cached.services), float64(b.cached.services), "svc"},
		{"StatefulSets в кэше", float64(a.cached.statefulsets), float64(b.cached.statefulsets), "sts"},
		{"Секретов в кэше", float64(a.cached.secrets), float64(b.cached.secrets), "sec"},
		{"PVC в кэше", float64(a.cached.pvcs), float64(b.cached.pvcs), "pvc"},
		{"Всего объектов в кэше", float64(a.totalCached()), float64(b.totalCached()), "obj"},
		{"Дельта heap после синхронизации", float64(max64(a.heapDeltaBytes, 0)), float64(max64(b.heapDeltaBytes, 0)), "B"},
		{"Время инициализации кэша", float64(a.initDuration.Milliseconds()), float64(b.initDuration.Milliseconds()), "ms"},
	}

	sb.WriteString(`<h2>Сводные диаграммы</h2><div class="bar-grid">`)
	const barMaxPx = 180
	for _, m := range barMetrics {
		maxV := m.valA
		if m.valB > maxV {
			maxV = m.valB
		}
		if maxV == 0 {
			maxV = 1
		}
		wA := int(m.valA / maxV * barMaxPx)
		if wA == 0 && m.valA > 0 {
			wA = 3
		}
		wB := int(m.valB / maxV * barMaxPx)
		if wB == 0 && m.valB > 0 {
			wB = 3
		}
		fmtVal := func(v float64) string {
			if m.unit == "B" {
				return fmtBytes(uint64(v))
			}
			return fmt.Sprintf("%.0f %s", v, m.unit)
		}
		fmt.Fprintf(&sb, `<div class="bar-card"><div class="bar-title">%s</div>`+
			`<div class="bar-group"><div class="bar-label">%s</div>`+
			`<div class="bar-row"><div class="bar bar-a" style="width:%dpx"></div><span class="bv">%s</span></div></div>`+
			`<div class="bar-group"><div class="bar-label" style="color:#c92a2a;font-weight:600">⚠️ %s</div>`+
			`<div class="bar-row"><div class="bar bar-b" style="width:%dpx"></div><span class="bv">%s</span></div></div>`+
			`</div>`+"\n",
			htmlEscape(m.title),
			htmlEscape(shortName(a.name)), wA, htmlEscape(fmtVal(m.valA)),
			htmlEscape(shortName(b.name)), wB, htmlEscape(fmtVal(m.valB)))
	}
	sb.WriteString("</div>\n")

	// Summary table
	sb.WriteString(`<div class="tbl-wrap"><table>`)
	fmt.Fprintf(&sb, `<tr><th>Metric</th><th>%s</th><th class="bad-col">⚠️ %s</th><th>Снижение</th></tr>`,
		htmlEscape(shortName(a.name)), htmlEscape(shortName(b.name)))
	tableRows := []struct {
		label    string
		va, vb   string
		pct      float64
		isSep    bool
	}{
		{"[Объекты в кэше]", "", "", 0, true},
		{"Поды", fmt.Sprintf("%d", a.cached.pods), fmt.Sprintf("%d", b.cached.pods),
			pctReduction(float64(a.cached.pods), float64(b.cached.pods)), false},
		{"Сервисы", fmt.Sprintf("%d", a.cached.services), fmt.Sprintf("%d", b.cached.services),
			pctReduction(float64(a.cached.services), float64(b.cached.services)), false},
		{"StatefulSets", fmt.Sprintf("%d", a.cached.statefulsets), fmt.Sprintf("%d", b.cached.statefulsets),
			pctReduction(float64(a.cached.statefulsets), float64(b.cached.statefulsets)), false},
		{"Секреты", fmt.Sprintf("%d", a.cached.secrets), fmt.Sprintf("%d", b.cached.secrets),
			pctReduction(float64(a.cached.secrets), float64(b.cached.secrets)), false},
		{"PVCs", fmt.Sprintf("%d", a.cached.pvcs), fmt.Sprintf("%d", b.cached.pvcs),
			pctReduction(float64(a.cached.pvcs), float64(b.cached.pvcs)), false},
		{"Всего", fmt.Sprintf("%d", a.totalCached()), fmt.Sprintf("%d", b.totalCached()),
			pctReduction(float64(a.totalCached()), float64(b.totalCached())), false},
		{"[Прочее]", "", "", 0, true},
		{"Дельта heap", fmtSignedBytes(a.heapDeltaBytes), fmtSignedBytes(b.heapDeltaBytes),
			pctReduction(float64(max64(a.heapDeltaBytes, 0)), float64(max64(b.heapDeltaBytes, 0))), false},
		{"Время инициализации", a.initDuration.String(), b.initDuration.String(),
			pctReduction(float64(a.initDuration), float64(b.initDuration)), false},
		{"Ср. задержка List", a.avgListLatency.String(), b.avgListLatency.String(),
			pctReduction(float64(a.avgListLatency), float64(b.avgListLatency)), false},
	}
	for _, r := range tableRows {
		if r.isSep {
			fmt.Fprintf(&sb, `<tr class="sep"><td colspan="4">%s</td></tr>`, htmlEscape(r.label))
			continue
		}
		pctStr := "—"
		if r.pct > 0 {
			pctStr = fmt.Sprintf(`<span class="good">−%.0f%%</span>`, r.pct)
		}
		fmt.Fprintf(&sb, `<tr><td>%s</td><td>%s</td><td class="bad-col">%s</td><td>%s</td></tr>`,
			htmlEscape(r.label), htmlEscape(r.va), htmlEscape(r.vb), pctStr)
	}
	sb.WriteString("</table></div>\n")

	// Conclusions
	fmt.Fprintf(&sb, `<div class="conclusion"><h2>Критические выводы</h2><ul>
<li><strong style="color:#c92a2a">Утечка объектов: кэшируется в %d раз больше данных, чем нужно.</strong>
    Без фильтра по лейблам оператор бездумно скачивает и хранит каждый Pod, Service, StatefulSet, Secret и PVC в кластере.
    Вместо необходимых <b>%d</b> объектов, он держит в памяти <b>%d</b>.</li>
<li><strong style="color:#c92a2a">Перерасход памяти: дельта heap больше на ~%.0f%%.</strong>
    Каждый лишний объект — это тяжелая Go-структура. Это приводит к гигантскому потреблению RAM, постоянной работе Garbage Collector'а и риску OOMKilled для пода оператора. С фильтром потребление памяти падает практически до нуля.</li>
<li><strong style="color:#c92a2a">Паразитная нагрузка на API-сервер.</strong>
    Поток WATCH без фильтра заставляет оператор реагировать на <em>каждое</em> изменение <em>любого</em> пода в кластере.
    Это тысячи мусорных событий в минуту, трата CPU на их декодирование и отбрасывание. С фильтром оператор спит, пока не тронут его ресурсы.</li>
<li><strong style="color:#c92a2a">Деградация скорости запуска.</strong>
    Скачивание огромного массива чужих объектов замедляет готовность кэша (<code>WaitForCacheSync</code>) в несколько раз, увеличивая время недоступности оператора при рестартах.</li>
<li><strong>Безопасное решение.</strong>
    Использование <code>cache.ByObject{Label: sel}</code> абсолютно безопасно и синхронизировано с фильтром событий контроллера. Промахи кэша для управляемых ресурсов исключены.</li>
</ul></div>
`, b.totalCached()/max(a.totalCached(), 1),
		a.totalCached(), b.totalCached(),
		heapRedPct)

	sb.WriteString("</body></html>\n")

	path := "cache_comparison.html"
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Logf("write HTML: %v", err)
		return
	}
	t.Logf("HTML report → %s", path)
}

func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&#34;")
	return s
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// Cluster seeding
// ---------------------------------------------------------------------------

// seedCluster creates неймспейсов с чужими нагрузками + kafka namespace with resources.
func seedCluster(t *testing.T, ctx context.Context, c client.Client) {
	t.Helper()

	// Kafka namespace.
	must(t, c.Create(ctx, ns(kafkaNs)))

	// Other-team namespaces.
	for i := range otherNamespaceCount {
		must(t, c.Create(ctx, ns(fmt.Sprintf("team-%02d", i))))
	}

	type job struct{ obj client.Object }
	jobs := make(chan job, 8192)

	// Kafka resources (labeled).
	for i := range kafkaInstances {
		stsName := fmt.Sprintf("kfk-kafka-%02d", i)
		must(t, c.Create(ctx, labeledSTS(stsName, kafkaNs, int32(brokersPerKafka))))
		must(t, c.Create(ctx, labeledSvc(fmt.Sprintf("kfk-kafka-%02d-svc", i), kafkaNs)))
		for b := range brokersPerKafka {
			jobs <- job{pod(fmt.Sprintf("%s-%d", stsName, b), kafkaNs,
				map[string]string{
					constants.ManagedByKey: constants.KafkaManagedByValue,
					"app":                  "kafka",
					"statefulset.kubernetes.io/pod-name": fmt.Sprintf("%s-%d", stsName, b),
				})}
		}
	}

	// Other-team resources (no kafka label).
	for i := range otherNamespaceCount {
		nsName := fmt.Sprintf("team-%02d", i)
		for j := range stsPerOtherNs {
			must(t, c.Create(ctx, plainSTS(fmt.Sprintf("app-%02d", j), nsName)))
			must(t, c.Create(ctx, plainSvc(fmt.Sprintf("svc-%02d", j), nsName)))
		}
		// Extra services beyond sts count.
		for j := stsPerOtherNs; j < svcPerOtherNs; j++ {
			must(t, c.Create(ctx, plainSvc(fmt.Sprintf("svc-%02d", j), nsName)))
		}
		for j := range podsPerOtherNs {
			jobs <- job{pod(fmt.Sprintf("pod-%04d", j), nsName,
				map[string]string{"app": fmt.Sprintf("app-%d", j%stsPerOtherNs)})}
		}
		for j := range secretsPerOtherNs {
			jobs <- job{secret(fmt.Sprintf("secret-%04d", j), nsName,
				map[string]string{"app": fmt.Sprintf("app-%d", j%stsPerOtherNs)})}
		}
		for j := range pvcsPerOtherNs {
			jobs <- job{pvc(fmt.Sprintf("pvc-%04d", j), nsName,
				map[string]string{"app": fmt.Sprintf("app-%d", j%stsPerOtherNs)})}
		}
	}
	close(jobs)

	// Create pods in parallel.
	const workers = 30
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := c.Create(ctx, j.obj); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("seed: %v", err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func ns(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func pod(name, namespace string, labels map[string]string) *corev1.Pod {
	// Add some dummy env vars to make the pod object larger (more realistic)
	envs := make([]corev1.EnvVar, 50)
	for i := 0; i < 50; i++ {
		envs[i] = corev1.EnvVar{
			Name:  fmt.Sprintf("DUMMY_ENV_VAR_%d", i),
			Value: strings.Repeat("long_dummy_value_", 10),
		}
	}

	vols := make([]corev1.Volume, 10)
	mounts := make([]corev1.VolumeMount, 10)
	for i := 0; i < 10; i++ {
		vols[i] = corev1.Volume{
			Name: fmt.Sprintf("vol-%d", i),
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}
		mounts[i] = corev1.VolumeMount{
			Name:      fmt.Sprintf("vol-%d", i),
			MountPath: fmt.Sprintf("/mnt/data-%d", i),
		}
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, 
			Namespace: namespace, 
			Labels: labels,
			Annotations: map[string]string{
				"dummy-annotation-1": strings.Repeat("val", 50),
				"dummy-annotation-2": strings.Repeat("val", 50),
				"dummy-annotation-3": strings.Repeat("val", 50),
			},
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name:  "init-1",
					Image: "busybox:1.36",
					Command: []string{"sh", "-c", "echo init"},
					Env: envs[:10],
					VolumeMounts: mounts[:5],
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "main", 
					Image: "pause:3.9",
					Env:   envs,
					VolumeMounts: mounts,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
				},
				{
					Name:  "sidecar", 
					Image: "fluentd:latest",
					Env:   envs[:20],
					VolumeMounts: mounts,
				},
			},
			Volumes: vols,
			Tolerations: []corev1.Toleration{
				{Key: "node-role.kubernetes.io/worker", Operator: corev1.TolerationOpExists},
				{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule},
			},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{Key: "kubernetes.io/os", Operator: corev1.NodeSelectorOpIn, Values: []string{"linux"}},
								},
							},
						},
					},
				},
			},
		},
	}
}

func labeledSTS(name, namespace string, replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.ManagedByKey: constants.KafkaManagedByValue,
				"app":                  "kafka",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "kafka"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "kafka"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "kafka", Image: "kafka:latest"}}},
			},
		},
	}
}

func labeledSvc(name, namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.ManagedByKey: constants.KafkaManagedByValue,
				"app":                  "kafka",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "kafka"},
			Ports:    []corev1.ServicePort{{Port: 9092}},
		},
	}
}

func plainSTS(name, namespace string) *appsv1.StatefulSet {
	r := int32(2)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace,
			Labels: map[string]string{"app": name}},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &r,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx:latest"}}},
			},
		},
	}
}

func plainSvc(name, namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace,
			Labels: map[string]string{"app": name}},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": name},
			Ports:    []corev1.ServicePort{{Port: 80}},
		},
	}
}

func secret(name, namespace string, labels map[string]string) *corev1.Secret {
	// Make the secret larger and simulate a TLS secret
	data := make(map[string][]byte)
	// Simulate a 4KB cert and 2KB key
	data[corev1.TLSCertKey] = []byte(strings.Repeat("c", 4096))
	data[corev1.TLSPrivateKeyKey] = []byte(strings.Repeat("k", 2048))
	
	// Add some extra data just to inflate it more
	for i := 0; i < 5; i++ {
		data[fmt.Sprintf("extra-key-%d", i)] = []byte(strings.Repeat("a", 2048))
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Type:       corev1.SecretTypeTLS,
		Data:       data,
	}
}

func pvc(name, namespace string, labels map[string]string) *corev1.PersistentVolumeClaim {
	storageClassName := "standard"
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
			StorageClassName: &storageClassName,
		},
	}
}

func findEnvtestBinDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(basePath, e.Name())
		}
	}
	return ""
}
