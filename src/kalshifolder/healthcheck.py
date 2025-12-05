import clickhouse_connect
import sys


def main():
    try:
        client = clickhouse_connect.get_client(host="clickhouse", port=8123)
        client.command("SELECT 1")
        print("ok")
        sys.exit(0)
    except Exception as e:
        print(f"failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
