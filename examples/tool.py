"""Manual example: register a local `get_weather` tool and let the agent call it.

The agent runs in the cloud, but `get_weather` runs HERE on your machine. When
the agent decides it needs weather, the cloud sends a `tool_request`; the client
runs your function locally and POSTs the result back, then the run continues.

    export DRIVER_API_KEY=dr_xxxxxxxx
    python examples/weather_tool.py "what should I wear in Barcelona today?"

Optional:
    export DRIVER_BASE_URL=https://driver.tors.app
    export DRIVER_DEBUG=1   # dump raw events
"""

import os
import sys

from crtrs.driver import Driver, DriverError, define_tool


# A fake weather source. Swap the body for a real API call (requests, httpx, …);
# it runs locally, so it can use your network, keys, and secrets.
_FORECAST = {
    "barcelona": (24, "sunny"),
    "london": (14, "rainy"),
    "oslo": (3, "snowy"),
}


def get_weather(city: str = "", units: str = "celsius"):
    """Return the current weather for a city. Runs locally on tool_request."""
    temp_c, sky = _FORECAST.get(city.strip().lower(), (20, "clear"))
    temp = temp_c if units == "celsius" else round(temp_c * 9 / 5 + 32)
    return {"city": city, "temp": temp, "units": units, "conditions": sky}


def main() -> int:
    if not os.environ.get("DRIVER_API_KEY"):
        print("set DRIVER_API_KEY (dr_...) — get one from the dashboard", file=sys.stderr)
        return 2

    prompt = " ".join(sys.argv[1:]) or "what should I wear in Barcelona today?"

    weather = define_tool(
        name="get_weather",
        description="Get the current weather for a city.",
        params=[
            {"name": "city", "description": "city name, e.g. 'Barcelona'"},
            {"name": "units", "type": "string", "description": "'celsius' or 'fahrenheit'"},
        ],
        call=get_weather,
    )

    driver = Driver(tools=[weather])  # reads DRIVER_API_KEY / DRIVER_BASE_URL from env
    debug = bool(os.environ.get("DRIVER_DEBUG"))

    print(f"prompt: {prompt}")
    done = None
    try:
        for ev in driver.stream(prompt):
            if debug:
                print("RAW", ev, file=sys.stderr)
            kind = ev["kind"]
            if kind == "plan":
                print("\n[plan]")
                for i, it in enumerate(ev.get("items") or []):
                    print(f"  {i + 1}. {it}")
            elif kind == "plan_item_start":
                print(f"\n[->] {ev['num'] + 1}. {ev['def']}")
            elif kind == "action":
                net = " [net]" if ev.get("is_network") else ""
                print(f"  . {ev['tool']}{net}")
            elif kind == "done":
                done = ev
    except DriverError as e:
        print(f"\nfatal: {e}", file=sys.stderr)
        return 1

    if done is not None:
        print(f"\n[done] steps={done.get('steps')} errors={done.get('errors')}")
        print("RESULT:", done.get("result"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
