# Template for the HighLoad course
This project is based on [Tiny Event Sourcing library](https://github.com/andrsuh/tiny-event-sourcing)

### Run PostgreSql
This example uses Postgres as an implementation of the Event store. You can see it in `pom.xml`:

```
<dependency>
    <groupId>ru.quipy</groupId>
    <artifactId>tiny-postgres-event-store-spring-boot-starter</artifactId>
    <version>${tiny.es.version}</version>
</dependency>
```

Thus, you have to run Postgres in order to test this example. Postgres service is included in  `docker-compose` file that we have in the root of the project.

# More comprehensive information about the course, project, how to run tests is here:

https://andrsuh.notion.site/2595d535059281d8a815c2cb3875c376?source=copy_link

https://andrsuh.notion.site/2625d5350592801aaf88c7c95302d10c?source=copy_link

### Run the infrastructure
Set of the services you need to start developing and testing process is following:
- Bombardier - service that is in charge of emulation the store's clients activity (creates the incoming load). Also serves as a third-party payment system.
- Postgres DBMS
- Prometheus + Grafana - metrics collection and visualization services

You can run all beforementioned services by the following command:
```
docker compose -f docker-compose.yml up
```

### Run the application
To make the application run you can start the main class `OnlineShopApplication`. It is not being launched as a docker contained to simplify and speed up the devevopment process as it is easier for you to refactor the application and re-run it immediately in the IDE.


### Adaptive per-account payment timeout

Direct Java and Docker launches now enable the same adaptive policy by default. Each account has its own in-memory window of 256 external attempts, initialized from its advertised `averageProcessingTime` with 25% headroom. After 32 observations the policy estimates the p80 completion time with a Kaplan-Meier estimator, adds 25% headroom and smooths each update by 0.1. These dimensionless policy settings are configurable under `payment.retry.adaptive` (`window-size`, `min-samples`, `quantile`, `headroom`, `smoothing`, `enabled`). The p80 target deliberately allows a slow tail; it is a tuning policy, not a guarantee of a particular success or retry rate.

Successful calls provide measured HTTP durations (excluding admission and event-store work). Confirmed provider timeouts and client timeout exceptions provide censored lower bounds, not successful durations. Other failures do not train the latency estimator. If censoring hides the selected quantile, the estimate grows beyond the observed lower bounds so it can discover a slower healthy provider. Observations age out by count, and state resets on process restart. Replicas learn independently.

Each attempt snapshots the estimate, acquires its account rate permit and clips the provider timeout to the remaining payment deadline minus `PAYMENT_RESPONSE_TIMEOUT_RESERVE` (default `300ms`). The response reserve remains a configurable transport allowance, not a learned value. A large estimate does not by itself prevent a shorter deadline-bounded attempt. Admission-queue capacity estimates still use the advertised account averages.

Set `PAYMENT_ATTEMPT_TIMEOUT` to a positive duration to override adaptation with a fixed per-attempt timeout. With `PAYMENT_ATTEMPT_TIMEOUT=0ms` (the default), adaptation is enabled; to restore the legacy full-deadline behavior also set `PAYMENT_RETRY_ADAPTIVE_ENABLED=false`. Unknown network outcomes are not retried. A provider HTTP 408 is retried only when enabled in `PAYMENT_RETRY_HTTP_STATUSES` and the body confirms `result=false` with message `Timeout`. Retry count and account rate limits still apply.

For a local run, keep the existing service name and token and use:

```sh
PAYMENT_ACCOUNTS=acc-7 PAYMENT_HOST=127.0.0.1 java -jar target/demo-0.0.1-SNAPSHOT.jar
```

Remove any old positive `PAYMENT_ATTEMPT_TIMEOUT` from the environment when testing adaptation. The startup log shows the effective mode and initial timeout. Metrics `payment_adaptive_timeout_seconds`, `payment_adaptive_samples`, and `payment_adaptive_censored_fraction` expose per-account policy state; `payment_attempt_duration_seconds` records HTTP attempt duration by account and outcome.

For remote testing, select the branch containing the adaptive implementation in `test-on-prem-run.http`, run the two cases sequentially, and wait for all terminal payment outcomes before starting the next case.

### Payment retry metric

`payment_retries_total{account,reason}` counts repeated HTTP attempts started after the first attempt for a payment. It increments immediately before executing the HTTP call, after deadline checks and rate admission. A retry cancelled before execution is not counted; a started call that subsequently fails to connect is counted. Reasons identify the previous retryable response (`http_408`, `http_429`, other configured HTTP statuses, or `temporary_error`), without payment IDs or provider message text in labels.

Use `sum by (account, reason) (increase(payment_retries_total[$__range]))` for a time window or `sum by (account, reason) (rate(payment_retries_total[$__rate_interval]))` for retries per second. `increase` is extrapolated; use counter differences across known run boundaries for exact test totals.

### If you want to pull changes from the main repository into your fork

The command ```git remote -v``` should include the following lines:

```
upstream        https://github.com/andrsuh/high-load-course.git (fetch)
upstream        https://github.com/andrsuh/high-load-course.git (push)
```

If not, add the upstream remote:
```git remote add upstream https://github.com/andrsuh/high-load-course.git```

To pull changes from the main repository, run the following commands:

```
git fetch upstream
# switch to the main branch of your fork. Make sure the branch has no uncommitted changes to avoid conflicts
git checkout main 
# merge changes from the main repository into your main branch
git merge upstream/main 
```