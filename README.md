# crtrs-driver

Python client for Driver. Run agents, stream events.

```sh
pip install crtrs-driver
```

```python
from crtrs.driver import Driver

driver = Driver(api_key="dr_...")  # or DRIVER_API_KEY

# Stream events.
for ev in driver.stream("what is https://ycombinator.com about?"):
    if ev["kind"] == "action":
        print(ev["tool"])

# ...or run to completion.
done = driver.run("what is https://ycombinator.com about?")
print(done["result"])
```

Events: `plan`, `plan_item_start`, `action`, `done`, `fatal`.

MIT
