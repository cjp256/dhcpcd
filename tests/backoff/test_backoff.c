/* SPDX-License-Identifier: BSD-2-Clause */
/*
 * test_backoff.c - test dhcpcd timings related to backoff behavior
 * Copyright (c) 2026 Microsoft Corporation
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Test for dhcpcd DHCPDISCOVER backoff timing.
 *
 * Runs N parallel dhcpcd instances, each in its own network namespace
 * with no DHCP server, and verifies the exponential backoff intervals
 * from the debug log match the expected RFC 2131 behaviour.
 *
 * Tests both default backoff (4/8/16/32/64) and configurable
 * initial_interval / backoff_cutoff options.
 *
 * Must be run as root.
 *
 * Usage: ./test_backoff [-b path-to-dhcpcd] [-n num-runs]
 */

#include <errno.h>
#include <getopt.h>
#include <limits.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Defaults from dhcp.h */
#define DHCP_BASE	4
#define DHCP_MAX	64
#define DFLT_JITTER_MS	1000		/* ±1s RFC default */

#define MAX_INTERVALS	20
#define MAX_RETRIES	10		/* max retry positions to track */
#define MAX_RUNS	256
#define MAX_PARALLEL	256		/* concurrent namespaces */
#define NS_PREFIX	"testbackoff"
#define IF_NAME		"testbackoff0"

static const char	*dhcpcd_bin;
static char		 tmpdir[256];
static int		 failures;
static int		 num_runs = 128;
static int		 verbose;
static unsigned int	 scenarios;	/* bitmask of scenarios to run */

#define SC_DEFAULTS	(1u << 0)
#define SC_MINLAT	(1u << 1)
#define SC_CLOUD	(1u << 2)
#define SC_REJECT	(1u << 3)
#define SC_ALL		(SC_DEFAULTS | SC_MINLAT | SC_CLOUD | SC_REJECT)

struct backoff_cfg {
	const char	*label;
	int		 initial_interval;	/* -1 = use default */
	int		 backoff_cutoff;	/* -1 = use default */
	int		 jitter_ms;		/* -1 = use default */
	unsigned int	 timeout_sec;
	unsigned int	 min_retries;		/* minimum expected retries */
	int		 nodelay;		/* 1 = emit nodelay in conf */
};

/* Per-run result */
struct run_result {
	double		intervals[MAX_INTERVALS];
	int		niv;
	double		initial_delay;		/* -1 = not present */
};

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static int
run_cmd(const char *cmd)
{
	int r;

	r = system(cmd);
	if (r == -1) {
		fprintf(stderr, "system(%s): %s\n", cmd, strerror(errno));
		return -1;
	}
	return WIFEXITED(r) ? WEXITSTATUS(r) : -1;
}

static void
cleanup_ns(const char *ns)
{
	char cmd[1024];

	snprintf(cmd, sizeof(cmd),
	    "ip netns pids %s 2>/dev/null | xargs -r kill -9 2>/dev/null; "
	    "rm -rf %s/%s_state 2>/dev/null; "
	    "ip netns del %s 2>/dev/null",
	    ns, tmpdir, ns, ns);
	run_cmd(cmd);
}

static int
setup_ns(const char *ns, const char *ifname)
{
	char cmd[1024];

	cleanup_ns(ns);

	snprintf(cmd, sizeof(cmd), "ip netns add %s", ns);
	if (run_cmd(cmd) != 0)
		return -1;

	snprintf(cmd, sizeof(cmd),
	    "ip netns exec %s ip link add %s type dummy", ns, ifname);
	if (run_cmd(cmd) != 0)
		goto fail;

	snprintf(cmd, sizeof(cmd),
	    "ip netns exec %s ip link set %s up", ns, ifname);
	if (run_cmd(cmd) != 0)
		goto fail;

	snprintf(cmd, sizeof(cmd),
	    "ip netns exec %s ip link set lo up", ns);
	run_cmd(cmd);

	/* Create per-namespace state dirs for dhcpcd */
	snprintf(cmd, sizeof(cmd),
	    "mkdir -p %s/%s_state/run %s/%s_state/db",
	    tmpdir, ns, tmpdir, ns);
	run_cmd(cmd);

	return 0;

fail:
	cleanup_ns(ns);
	return -1;
}

/*
 * Write a dhcpcd.conf, run dhcpcd inside the namespace, and collect
 * the "next in X.X seconds" intervals from its debug log.
 *
 * Returns the number of intervals parsed, or -1 on error.
 */
static int
run_dhcpcd(const struct backoff_cfg *cfg,
    const char *ns, const char *ifname,
    double *intervals, int max_iv, double *initial_delay)
{
	char confpath[PATH_MAX], logpath[PATH_MAX],
	    cmd[PATH_MAX * 4], line[512];
	FILE *conf, *logfp;
	int n;
	char *p;
	double v;

	*initial_delay = -1.0;

	snprintf(confpath, sizeof(confpath), "%s/%s.conf", tmpdir, ns);
	snprintf(logpath, sizeof(logpath), "%s/%s.log", tmpdir, ns);

	conf = fopen(confpath, "w");
	if (conf == NULL) {
		fprintf(stderr, "fopen(%s): %s\n", confpath, strerror(errno));
		return -1;
	}
	if (cfg->nodelay)
		fprintf(conf, "nodelay\n");
	fprintf(conf,
	    "noipv4ll\n"
	    "noipv6\n"
	    "nohook *\n"
	    "reboot 0\n"
	    "timeout %u\n", cfg->timeout_sec);

	if (cfg->initial_interval >= 0)
		fprintf(conf, "initial_interval %d\n",
		    cfg->initial_interval);
	if (cfg->backoff_cutoff >= 0)
		fprintf(conf, "backoff_cutoff %d\n", cfg->backoff_cutoff);
	if (cfg->jitter_ms >= 0)
		fprintf(conf, "backoff_jitter %d\n", cfg->jitter_ms);
	fclose(conf);

	/* External timeout is required when using -B.
	 * Bind-mount private state dirs inside the same ip-netns-exec
	 * invocation so parallel instances don't collide. */
	snprintf(cmd, sizeof(cmd),
	    "timeout --signal=TERM %u "
	    "ip netns exec %s sh -c '"
	    "mount --bind %s/%s_state/run /var/run/dhcpcd && "
	    "mount --bind %s/%s_state/db /var/db/dhcpcd && "
	    "exec %s -B -d -1 -f %s %s' >%s 2>&1",
	    cfg->timeout_sec,
	    ns, tmpdir, ns, tmpdir, ns, dhcpcd_bin, confpath, ifname,
	    logpath);

	run_cmd(cmd);

	/* Parse intervals from log */
	logfp = fopen(logpath, "r");
	if (logfp == NULL) {
		fprintf(stderr, "fopen(%s): %s\n", logpath, strerror(errno));
		unlink(confpath);
		return -1;
	}

	n = 0;
	while (fgets(line, sizeof(line), logfp) != NULL) {
		/* Capture "delaying IPv4 for X.X seconds" */
		p = strstr(line, "delaying IPv4 for ");
		if (p != NULL) {
			p += 18; /* strlen("delaying IPv4 for ") */
			v = strtod(p, NULL);
			if (v >= 0.0)
				*initial_delay = v;
			continue;
		}
		if (n >= max_iv)
			continue;
		p = strstr(line, "next in ");
		if (p == NULL)
			continue;
		p += 8; /* strlen("next in ") */
		v = strtod(p, NULL);
		if (v > 0.0)
			intervals[n++] = v;
	}

	fclose(logfp);
	unlink(confpath);
	unlink(logpath);
	return n;
}

/* ------------------------------------------------------------------ */
/* Single run in a child process — writes result to a temp file        */
/* ------------------------------------------------------------------ */

static void
do_one_run(const struct backoff_cfg *cfg, unsigned int parent_pid,
    unsigned int idx)
{
	char ns[64], outpath[PATH_MAX];
	const char *ifname = IF_NAME;
	double intervals[MAX_INTERVALS];
	double initial_delay;
	int niv, i;
	FILE *fp;

	snprintf(ns, sizeof(ns), "%s_%u_%u",
	    NS_PREFIX, parent_pid, idx);
	snprintf(outpath, sizeof(outpath), "%s/%s.out", tmpdir, ns);

	fp = fopen(outpath, "w");
	if (fp == NULL)
		_exit(1);

	if (setup_ns(ns, ifname) != 0) {
		fprintf(fp, "SKIP\n");
		fclose(fp);
		cleanup_ns(ns);
		_exit(0);
	}

	niv = run_dhcpcd(cfg, ns, ifname, intervals, MAX_INTERVALS,
	    &initial_delay);
	cleanup_ns(ns);

	if (niv <= 0) {
		fprintf(fp, "SKIP\n");
	} else {
		fprintf(fp, "DELAY:%.3f\n", initial_delay);
		for (i = 0; i < niv; i++)
			fprintf(fp, "%.1f\n", intervals[i]);
	}
	fclose(fp);
	_exit(0);
}

/* ------------------------------------------------------------------ */
/* Collect results from child output files                             */
/* ------------------------------------------------------------------ */

static int
collect_results(unsigned int parent_pid, int nruns,
    struct run_result *results)
{
	int good = 0;
	int i, j;

	for (i = 0; i < nruns; i++) {
		char outpath[PATH_MAX], line[64];
		FILE *fp;
		int skip;

		snprintf(outpath, sizeof(outpath), "%s/%s_%u_%u.out",
		    tmpdir, NS_PREFIX, parent_pid, (unsigned)i);

		fp = fopen(outpath, "r");
		if (fp == NULL)
			continue;

		skip = 0;
		j = 0;
		results[good].initial_delay = -1.0;
		while (fgets(line, sizeof(line), fp) != NULL &&
		    j < MAX_INTERVALS)
		{
			if (strncmp(line, "SKIP", 4) == 0) {
				skip = 1;
				break;
			}
			if (strncmp(line, "DELAY:", 6) == 0) {
				results[good].initial_delay =
				    strtod(line + 6, NULL);
				continue;
			}
			results[good].intervals[j++] = strtod(line, NULL);
		}
		fclose(fp);
		unlink(outpath);

		if (!skip && j > 0) {
			results[good].niv = j;
			good++;
		}
	}
	return good;
}

/* ------------------------------------------------------------------ */
/* Assertions                                                          */
/* ------------------------------------------------------------------ */

static void
check(int ok, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	if (ok) {
		printf("  PASS: ");
	} else {
		printf("  FAIL: ");
		failures++;
	}
	vprintf(fmt, ap);
	printf("\n");
	va_end(ap);
}

static double
jitter_tolerance(const struct backoff_cfg *cfg)
{
	unsigned int jms;

	jms = cfg->jitter_ms >= 0 ? (unsigned)cfg->jitter_ms : DFLT_JITTER_MS;
	return (double)jms / 1000.0;
}

/* ------------------------------------------------------------------ */
/* Compute expected base interval for a given retry position           */
/* pos=0 is the first interval reported (delay before 2nd DISCOVER)    */
/* ------------------------------------------------------------------ */

static unsigned int
expected_interval(const struct backoff_cfg *cfg, int pos)
{
	unsigned int base, cap, interval;
	int i;

	base = cfg->initial_interval >= 0
	    ? (unsigned)cfg->initial_interval : DHCP_BASE;
	cap  = cfg->backoff_cutoff >= 0
	    ? (unsigned)cfg->backoff_cutoff : DHCP_MAX;

	interval = base;
	for (i = 0; i < pos; i++) {
		interval *= 2;
		if (interval > cap)
			interval = cap;
	}
	return interval;
}

/* ------------------------------------------------------------------ */
/* Verify results                                                      */
/* ------------------------------------------------------------------ */

static int
cmp_double(const void *a, const void *b)
{
	double da = *(const double *)a;
	double db = *(const double *)b;

	if (da < db) return -1;
	if (da > db) return  1;
	return 0;
}

static void
verify_stats(const struct backoff_cfg *cfg,
    const struct run_result *results, int good)
{
	int pos, max_pos, r;
	double vals[MAX_RUNS];
	char expbuf[32], minbuf[16], avgbuf[16], maxbuf[16];

	/* Find the maximum retry position across all runs */
	max_pos = 0;
	for (r = 0; r < good; r++) {
		if (results[r].niv > max_pos)
			max_pos = results[r].niv;
	}
	if (max_pos > MAX_RETRIES)
		max_pos = MAX_RETRIES;

	check(good == num_runs, "%d/%d runs produced data", good, num_runs);

	printf("\n  %-8s  %-10s  %-7s  %-7s  %-7s  %-4s  %s\n",
	    "Retry", "Expected", "Min", "Avg", "Max", "N", "Result");
	printf("  ------------------------------------------------------\n");

	/* Verify initial delay across all runs */
	if (!cfg->nodelay) {
		int delay_count = 0, delay_ok = 0;
		double delay_avg = 0.0;

		for (r = 0; r < good; r++) {
			if (results[r].initial_delay >= 0.0)
				vals[delay_count++] =
				    results[r].initial_delay;
		}
		if (delay_count > 0) {
			qsort(vals, (size_t)delay_count,
			    sizeof(double), cmp_double);
			for (r = 0; r < delay_count; r++) {
				if (vals[r] >= 0.0 && vals[r] <= 2.0)
					delay_ok++;
				delay_avg += vals[r];
			}
			delay_avg /= delay_count;

			snprintf(expbuf, sizeof(expbuf),
			    "%u\xc2\xb1%.1fs", 1, 1.0);
			snprintf(minbuf, sizeof(minbuf),
			    "%.1fs", vals[0]);
			snprintf(avgbuf, sizeof(avgbuf),
			    "%.1fs", delay_avg);
			snprintf(maxbuf, sizeof(maxbuf),
			    "%.1fs", vals[delay_count - 1]);
			printf("  %-8s  %-11s  %-7s  %-7s  %-7s  %-4d  %s\n",
			    "Init", expbuf,
			    minbuf, avgbuf, maxbuf,
			    delay_count,
			    delay_ok == delay_count ? "PASS" : "FAIL");
			if (verbose) {
				printf("    observed values:");
				for (r = 0; r < delay_count; r++)
					printf(" %.1fs", vals[r]);
				printf("\n");
			}
			if (delay_ok != delay_count)
				failures++;
		} else {
			check(0, "initial delay: no data captured");
		}
	} else {
		int nodelay_ok = 1;

		for (r = 0; r < good; r++) {
			if (results[r].initial_delay >= 0.0) {
				nodelay_ok = 0;
				break;
			}
		}
		printf("  %-8s  %-10s  %-7s  %-7s  %-7s  %-4d  %s\n",
		    "Init", "0", "-", "-", "-", good,
		    nodelay_ok ? "PASS" : "FAIL");
		if (!nodelay_ok)
			failures++;
	}

	for (pos = 0; pos < max_pos; pos++) {
		int count, in_range, ok;
		unsigned int exp;
		double lo, hi, jtol, avg;

		/* Collect values at this retry position */
		count = 0;
		for (r = 0; r < good; r++) {
			if (results[r].niv > pos)
				vals[count++] = results[r].intervals[pos];
		}
		if (count == 0)
			continue;

		qsort(vals, (size_t)count, sizeof(double), cmp_double);

		exp = expected_interval(cfg, pos);
		jtol = jitter_tolerance(cfg);
		lo = (double)exp - jtol;
		hi = (double)exp + jtol;

		in_range = 0;
		avg = 0.0;
		for (r = 0; r < count; r++) {
			if (vals[r] >= lo && vals[r] <= hi)
				in_range++;
			avg += vals[r];
		}
		avg /= count;

		ok = (in_range == count);
		snprintf(expbuf, sizeof(expbuf),
		    "%u\xc2\xb1%.1fs", exp, jtol);
		snprintf(minbuf, sizeof(minbuf),
		    "%.1fs", vals[0]);
		snprintf(avgbuf, sizeof(avgbuf),
		    "%.1fs", avg);
		snprintf(maxbuf, sizeof(maxbuf),
		    "%.1fs", vals[count - 1]);
		printf("  %-8d  %-11s  %-7s  %-7s  %-7s  %-4d  %s\n",
		    pos + 1, expbuf,
		    minbuf, avgbuf, maxbuf,
		    count,
		    ok ? "PASS" : "FAIL");

		if (verbose) {
			printf("    observed values:");
			for (r = 0; r < count; r++)
				printf(" %.1fs", vals[r]);
			printf("\n");
		}

		if (!ok)
			failures++;
	}
}

/* ------------------------------------------------------------------ */
/* Test: run N instances and verify                                    */
/* ------------------------------------------------------------------ */

static void
test_backoff(const struct backoff_cfg *cfg)
{
	struct run_result *results;
	unsigned int ppid;
	int i, good, batch;
	struct timespec t0, t1;
	double elapsed;

	printf("\n--- %s (N=%d) ---\n", cfg->label, num_runs);
	printf("  dhcpcd.conf: timeout=%u", cfg->timeout_sec);
	if (cfg->nodelay)
		printf(", nodelay");
	if (cfg->initial_interval >= 0)
		printf(", initial_interval=%d",
		    cfg->initial_interval);
	if (cfg->backoff_cutoff >= 0)
		printf(", backoff_cutoff=%d", cfg->backoff_cutoff);
	if (cfg->jitter_ms >= 0)
		printf(", backoff_jitter=%d", cfg->jitter_ms);
	printf("\n");

	clock_gettime(CLOCK_MONOTONIC, &t0);

	results = calloc((size_t)num_runs, sizeof(*results));
	if (results == NULL) {
		fprintf(stderr, "calloc: %s\n", strerror(errno));
		failures++;
		return;
	}

	ppid = (unsigned)getpid();

	/* Launch runs in parallel batches */
	batch = 0;
	for (i = 0; i < num_runs; i++) {
		pid_t pid = fork();

		if (pid == 0) {
			do_one_run(cfg, ppid, (unsigned)i);
			_exit(0);
		} else if (pid < 0) {
			fprintf(stderr, "fork: %s\n", strerror(errno));
			continue;
		}

		batch++;
		if (batch >= MAX_PARALLEL) {
			while (wait(NULL) > 0)
				;
			batch = 0;
		}
	}
	/* Wait for remaining children */
	while (wait(NULL) > 0)
		;

	good = collect_results(ppid, num_runs, results);

	if (good == 0) {
		fprintf(stderr, "  FAIL: no successful runs\n");
		failures++;
		free(results);
		return;
	}

	verify_stats(cfg, results, good);

	clock_gettime(CLOCK_MONOTONIC, &t1);
	elapsed = (double)(t1.tv_sec - t0.tv_sec) +
	    (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
	printf("  Elapsed: %.1fs\n", elapsed);

	free(results);
}

/* ------------------------------------------------------------------ */
/* Test: verify that dhcpcd rejects an invalid config option value      */
/* ------------------------------------------------------------------ */

static void
test_reject(const char *label, const char *option, const char *value)
{
	char confpath[PATH_MAX], logpath[PATH_MAX],
	    cmd[PATH_MAX * 4], line[512],
	    errmsg[512];
	FILE *conf, *logfp;
	int rc, found;

	printf("\n--- %s ---\n", label);
	printf("  dhcpcd.conf: %s %s (expect rejection)\n", option, value);

	snprintf(confpath, sizeof(confpath), "%s/reject-%s.conf",
	    tmpdir, option);
	snprintf(logpath, sizeof(logpath), "%s/reject-%s.log",
	    tmpdir, option);

	conf = fopen(confpath, "w");
	if (conf == NULL) {
		fprintf(stderr, "fopen(%s): %s\n", confpath, strerror(errno));
		failures++;
		return;
	}
	fprintf(conf, "%s %s\n", option, value);
	fclose(conf);

	/* Run in a private mount+net namespace with state dirs bind-mounted
	 * from tmpdir, so a parse-time failure cannot disturb the host's
	 * real /var/run/dhcpcd or /var/db/dhcpcd.
	 *
	 * An invalid config value is logged but non-fatal, so dhcpcd keeps
	 * running and waits for carrier.  Wrap it in a timeout so the command
	 * returns; the rejection is verified from the log, not the exit code. */
	snprintf(cmd, sizeof(cmd),
	    "mkdir -p %s/reject_state/run %s/reject_state/db && "
	    "timeout --signal=TERM 5 "
	    "unshare --mount --net --uts --ipc --pid --fork sh -c '"
	    "mkdir -p /var/run/dhcpcd /var/db/dhcpcd && "
	    "mount --bind %s/reject_state/run /var/run/dhcpcd && "
	    "mount --bind %s/reject_state/db /var/db/dhcpcd && "
	    "exec %s -B -d -1 -f %s lo' >%s 2>&1",
	    tmpdir, tmpdir, tmpdir, tmpdir,
	    dhcpcd_bin, confpath, logpath);

	rc = run_cmd(cmd);

	/* Check log for the expected error message */
	found = 0;
	errmsg[0] = '\0';
	logfp = fopen(logpath, "r");
	if (logfp != NULL) {
		while (fgets(line, sizeof(line), logfp) != NULL) {
			if (strstr(line, "invalid") != NULL) {
				found = 1;
				/* Save the error line, strip trailing newline */
				snprintf(errmsg, sizeof(errmsg), "%s", line);
				errmsg[strcspn(errmsg, "\n")] = '\0';
				break;
			}
		}
		fclose(logfp);
	}

	unlink(confpath);
	unlink(logpath);

	if (found) {
		printf("  PASS: dhcpcd rejected %s %s (exit=%d)\n",
		    option, value, rc);
		printf("    output: %s\n", errmsg);
	} else {
		printf("  FAIL: dhcpcd did not reject %s %s (exit=%d)\n",
		    option, value, rc);
		failures++;
	}
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

static void
usage(const char *prog)
{

	fprintf(stderr,
	    "Usage: %s [-b path-to-dhcpcd] [-n num-runs] [-s name] [-v]\n"
	    "\n"
	    "Options:\n"
	    "  -b path   Path to dhcpcd binary "
	    "(default: ../../src/dhcpcd)\n"
	    "  -n N      Number of parallel runs per test "
	    "(default: 128, max: %d)\n"
	    "  -s name   Run only scenario name "
	    "(defaults, min-latency, cloud, reject)\n"
	    "  -v        Print all observed values per retry\n"
	    "  -h        Show this help\n",
	    prog, MAX_RUNS);
}

int
main(int argc, char **argv)
{
	const char *bin = "../../src/dhcpcd";
	char resolved[PATH_MAX];
	int ch;
	struct stat st;

	struct backoff_cfg test_defaults = {
		.label			= "defaults",
		.initial_interval	= -1,
		.backoff_cutoff		= -1,
		.jitter_ms		= -1,
		.timeout_sec		= 75,
		.min_retries		= 3,
	};

	struct backoff_cfg test_min_latency = {
		.label		= "min-latency",
		.initial_interval = 1,
		.backoff_cutoff	= 1,
		.jitter_ms	= 0,
		.timeout_sec	= 12,
		.min_retries	= 4,
		.nodelay	= 1,
	};

	struct backoff_cfg test_cloud_recommended = {
		.label		= "cloud",
		.initial_interval = 1,
		.backoff_cutoff	= 1,
		.jitter_ms	= 100,
		.timeout_sec	= 12,
		.min_retries	= 4,
		.nodelay	= 1,
	};



	while ((ch = getopt(argc, argv, "b:n:s:vh")) != -1) {
		switch (ch) {
		case 'b':
			bin = optarg;
			break;
		case 'n':
			num_runs = atoi(optarg);
			break;
		case 's':
			if (strcmp(optarg, "defaults") == 0)
				scenarios |= SC_DEFAULTS;
			else if (strcmp(optarg, "min-latency") == 0)
				scenarios |= SC_MINLAT;
			else if (strcmp(optarg, "cloud") == 0)
				scenarios |= SC_CLOUD;
			else if (strcmp(optarg, "reject") == 0)
				scenarios |= SC_REJECT;
			else {
				fprintf(stderr,
				    "ERROR: unknown scenario '%s'\n"
				    "  valid: defaults, min-latency, "
				    "cloud, reject\n",
				    optarg);
				return 1;
			}
			break;
		case 'v':
			verbose = 1;
			break;
		case 'h':
			usage(argv[0]);
			return 0;
		default:
			usage(argv[0]);
			return 1;
		}
	}

	if (getuid() != 0) {
		fprintf(stderr, "ERROR: must be run as root (sudo).\n");
		return 1;
	}

	if (realpath(bin, resolved) == NULL) {
		fprintf(stderr,
		    "ERROR: dhcpcd not found at %s: %s\n",
		    bin, strerror(errno));
		return 1;
	}
	dhcpcd_bin = strdup(resolved);

	if (num_runs < 1)
		num_runs = 1;
	if (num_runs > MAX_RUNS)
		num_runs = MAX_RUNS;

	if (stat(dhcpcd_bin, &st) != 0 || !(st.st_mode & S_IXUSR)) {
		fprintf(stderr,
		    "ERROR: dhcpcd not executable at %s\n",
		    dhcpcd_bin);
		return 1;
	}

	/* Create temp directory for all test artifacts */
	snprintf(tmpdir, sizeof(tmpdir), "/tmp/testbackoff.XXXXXX");
	if (mkdtemp(tmpdir) == NULL) {
		fprintf(stderr, "mkdtemp: %s\n", strerror(errno));
		return 1;
	}

	/* Per-scenario state lives under tmpdir; do not touch the host's
	 * real /var/run/dhcpcd or /var/db/dhcpcd, and do not pkill any
	 * dhcpcd the user is running. */

	if (scenarios == 0)
		scenarios = SC_ALL;

	printf("dhcpcd DHCPDISCOVER backoff integration test\n");
	printf("=============================================\n");
	printf("Binary: %s\n", dhcpcd_bin);
	printf("Runs:   %d per test\n", num_runs);

	if (scenarios & SC_DEFAULTS)
		test_backoff(&test_defaults);
	if (scenarios & SC_MINLAT)
		test_backoff(&test_min_latency);
	if (scenarios & SC_CLOUD)
		test_backoff(&test_cloud_recommended);

	if (scenarios & SC_REJECT) {
		test_reject("reject initial_interval=0",
		    "initial_interval", "0");
		test_reject("reject backoff_cutoff=0",
		    "backoff_cutoff", "0");
		test_reject("reject initial_interval above max",
		    "initial_interval", "5");
		test_reject("reject backoff_cutoff above max",
		    "backoff_cutoff", "65");
		test_reject("reject backoff_jitter above max",
		    "backoff_jitter", "1001");
	}

	/* Remove temp directory */
	{
		char cmd[512];

		snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpdir);
		run_cmd(cmd);
	}

	printf("\n=============================================\n");
	if (failures == 0)
		printf("All tests passed.\n");
	else
		printf("%d test(s) FAILED.\n", failures);

	return failures ? 1 : 0;
}
