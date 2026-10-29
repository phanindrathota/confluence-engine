from confluence.ratelimit import RateLimiter


class FakeClock:
    def __init__(self):
        self.t = 1000.0
        self.slept = []

    def __call__(self):
        return self.t

    async def sleep(self, s):
        self.slept.append(s)
        self.t += s


async def test_budget_per_minute_enforced():
    c = FakeClock()
    rl = RateLimiter(budget_per_min=5, burst_per_sec=100, reserve=0, clock=c, sleep=c.sleep)
    for _ in range(5):
        await rl.acquire()
    assert c.slept == []
    await rl.acquire()                 # 6th must wait for the window
    assert sum(c.slept) >= 59.9


async def test_burst_cap():
    c = FakeClock()
    rl = RateLimiter(budget_per_min=1000, burst_per_sec=3, reserve=0, clock=c, sleep=c.sleep)
    for _ in range(4):
        await rl.acquire()
    assert 0.9 < sum(c.slept) < 1.1


async def test_server_reserve_slows_down():
    c = FakeClock()
    rl = RateLimiter(budget_per_min=1000, burst_per_sec=100, reserve=25, clock=c, sleep=c.sleep)
    rl.update_from_headers({"X-RateLimit-Remaining": "20", "X-RateLimit-Reset": "30"})
    await rl.acquire()
    assert sum(c.slept) > 0            # shared bucket nearly empty -> we yield


async def test_429_blocks_all_callers():
    c = FakeClock()
    rl = RateLimiter(budget_per_min=1000, burst_per_sec=100, reserve=0, clock=c, sleep=c.sleep)
    wait = rl.on_429(2)
    assert wait >= 2
    await rl.acquire()
    assert sum(c.slept) >= 2
