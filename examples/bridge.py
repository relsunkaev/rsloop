import asyncio

import kioto


async def main() -> None:
    await kioto.sleep(0.01)
    print(await kioto.run_in_tokio(asyncio.sleep(0.01, result="python future executed inside tokio")))


if __name__ == "__main__":
    kioto.run(main())
