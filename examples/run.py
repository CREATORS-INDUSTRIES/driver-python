"""Manual example: run a real prompt against Driver cloud and print events live.

    export DRIVER_API_KEY=dr_xxxxxxxx
    python examples/run.py "what is https://ycombinator.com about?"
    python examples/run.py --zdr "summarize this confidential brief"

--zdr runs with zero data retention (needs the account entitlement; the server
answers 403 without it).

Optional:
    export DRIVER_BASE_URL=https://driver.tors.app
    export DRIVER_DEBUG=1   # dump raw events

Bring-your-own engine (all three optional; omit to use the cloud default):
    export DRIVER_LLM_ENGINE=openrouter        # openai | mistral | claude | openrouter
    export DRIVER_LLM_MODEL=openai/gpt-oss-120b
    export DRIVER_LLM_API_KEY=sk-or-...        # the engine's key, NOT the dr_ credential
"""

import os
import sys

from crtrs.driver import Driver, DriverError


def main() -> int:
    if not os.environ.get("DRIVER_API_KEY"):
        print("set DRIVER_API_KEY (dr_...) — get one from the dashboard", file=sys.stderr)
        return 2

    args = sys.argv[1:]
    zdr = "--zdr" in args
    prompt = " ".join(a for a in args if a != "--zdr") or "what is https://ycombinator.com about?"
    driver = Driver(  # reads DRIVER_API_KEY / DRIVER_BASE_URL from env
        engine=os.environ.get("DRIVER_LLM_ENGINE"),
        model=os.environ.get("DRIVER_LLM_MODEL"),
        engine_key=os.environ.get("DRIVER_LLM_API_KEY"),
    )
    debug = bool(os.environ.get("DRIVER_DEBUG"))

    print(f"prompt: {prompt}{'  [zdr]' if zdr else ''}")
    done = None
    try:
        for ev in driver.stream(prompt, zdr=zdr):
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
        for d in done.get("data") or []:
            print(f"  {d['var']} ({d['label']}): {len(d['value'])} chars")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
