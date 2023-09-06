
1. Generate a baseline for the service `go test ./services/repository -bench . -benchmem -count=100 > ./services/repository/CreateRepository_baseline`
1. Make some code changes that may impact the service
1. Run the benchmark again `go test ./services/repository -bench . -benchmem -count=100 > ./services/repository/CreateRepository_new`
1. Use https://pkg.go.dev/golang.org/x/perf/cmd/benchstat to generate report of differences
  1. `go install golang.org/x/perf/cmd/benchstat@latest`
  1. `benchstat ./services/repository/CreateRepository_baseline ./services/repository/CreateRepository_new`



We need to look at the `vs base` values to see how the service behaves when compared to the baseline.
It's important to note both the delta, but also the p value which determines the statistical significance of the delta, with a lower value indicating a greater statistical significance


### Sample Report detecting difference

In this report we see +112.91% in sec/op, +44.12% B/op, +55.08% allocs/op all with a p=0.000 indicating a statistically significant change

```
goos: darwin
goarch: arm64
pkg: gitlab.com/gitlab-org/gitaly/v16/test/perf/services/repository
│ ./services/repository/CreateRepository_baseline │ ./services/repository/CreateRepository_new │
│                     sec/op                      │      sec/op        vs base                 │
_CreateRepository-10                                       95.62m ± 5%        203.59m ± 9%  +112.91% (p=0.000 n=10)

                     │ ./services/repository/CreateRepository_baseline │ ./services/repository/CreateRepository_new │
                     │                      B/op                       │        B/op         vs base                │
_CreateRepository-10                                     343.3Ki ± 11%        494.8Ki ± 11%  +44.12% (p=0.000 n=10)

                     │ ./services/repository/CreateRepository_baseline │ ./services/repository/CreateRepository_new │
                     │                    allocs/op                    │     allocs/op       vs base                │
_CreateRepository-10                                      2.567k ± 11%         3.981k ± 14%  +55.08% (p=0.000 n=10)

```


### Sample Report detecting no difference

In this report we note the deltas record a ~ which indicates no change in behaviour, (n=100 indicates 100 samples used)
```
goos: darwin
goarch: arm64
pkg: gitlab.com/gitlab-org/gitaly/v16/test/perf/services/repository
                     │ ./services/repository/CreateRepository_baseline │ ./services/repository/CreateRepository_new_no_diff │
                     │                     sec/op                      │             sec/op               vs base           │
_CreateRepository-10                                       87.56m ± 2%                       88.30m ± 2%  ~ (p=0.064 n=100)

                     │ ./services/repository/CreateRepository_baseline │ ./services/repository/CreateRepository_new_no_diff │
                     │                      B/op                       │              B/op                vs base           │
_CreateRepository-10                                      332.4Ki ± 3%                      332.1Ki ± 3%  ~ (p=0.909 n=100)

                     │ ./services/repository/CreateRepository_baseline │ ./services/repository/CreateRepository_new_no_diff │
                     │                    allocs/op                    │            allocs/op             vs base           │
_CreateRepository-10                                       2.462k ± 4%                       2.460k ± 4%  ~ (p=0.688 n=100)

```
