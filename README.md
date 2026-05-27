# Kubernetes Operator Cache Comparison

This is a standalone envtest project that demonstrates why a Kubernetes operator should use label-filtered controller-runtime caches.

The test compares two startup scenarios:

1. **Filtered cache**: `cache.ByObject{Label: selector}` is applied to Pods, Services, StatefulSets, Secrets and PVCs. Only objects managed by the operator are stored in informer caches.
2. **Unfiltered cache**: no label selector is applied. The operator cache receives every object of those types from the whole cluster.

The generated report focuses on the practical risk: without filtering, the operator stores thousands of unrelated cluster objects in memory, which increases heap usage, GC pressure and startup time.

## Simulated Cluster

The test starts a local Kubernetes API server and etcd via `envtest`, then creates:

- 50 namespaces that represent other teams.
- 5000 unrelated Pods.
- 1000 unrelated TLS-like Secrets.
- 200 unrelated PVCs.
- 100 unrelated Services.
- 50 unrelated StatefulSets.
- 1 Kafka namespace with 9 Pods, 3 Services and 3 StatefulSets that have the managed label.

## Metrics

The report includes:

- Heap delta during cache initialization.
- Objects stored in the local informer cache.
- Cache initialization duration.
- Average cached List latency.

The main HTML report is written to `cache_comparison.html`.

## Requirements

- Go 1.25.7 or compatible toolchain.
- Network access for the first dependency download.
- Optional: `weasyprint` for PDF generation.

## Usage

Run the comparison and generate the HTML report:

```sh
make test
```

Open the report:

```sh
make open
```

Generate a PDF from the report, if `weasyprint` is installed:

```sh
make pdf
```

Clean generated files and local envtest binaries:

```sh
make clean
```

## Notes

The first run downloads `setup-envtest`, `kube-apiserver` and `etcd` into the local `bin/` directory. Subsequent runs reuse those binaries.

