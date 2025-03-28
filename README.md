### Build

```shell
./build.sh
```

### Run

#### start

```shell
java -jar target/rocketmq-mcp-server.jar
```

#### check

```shell
tigerweili@M4 ~ % curl http://127.0.0.1:6868/sse
id:8e323b3b-cb73-4b35-8ac5-172b453eae79
event:endpoint
data:sse
```

### Use it

```json
{
  "mcpServers": {
    "rocketmq-mcp": {
      "url": "http://your-rocketmq-mcp-server-ip:6868/sse",
      "env": {
        "NS_ADDR": "1.1.1.1:9876;2.2.2.2:9876", 
        "AK": "",
        "SK": ""
      }
    }
  }
}

NS_ADDR: name server address list
AK: access key
SK: secret key
```
