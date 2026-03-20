import asyncio

import rsloop


async def main() -> None:
    await rsloop.sleep(0.01)
    print(await rsloop.run_in_tokio(asyncio.sleep(0.01, result="python future executed inside tokio")))


if __name__ == "__main__":
    rsloop.run(main())
