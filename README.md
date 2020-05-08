This connector allows Kafka Connect to receive data from network.  Connector implemented on top of [netty.io](https://netty.io/) (3.x).

# Configuration

## NettySourceConnector

The Source Connector will receive data from network  and write to kafka a topic.
```properties
name=nettyConnector
tasks.max=1
connector.class=com.mckesson.kafka.connect.nettysource.NettySourceConnector
topic=network_data

# Set these required values
transport.protocol=TCP
port=1234
```
| Name  | Description | Type     | Default | Importance |Notes
|---|---|---|---|---|---|
|bind.address|Bind address|string|0.0.0.0|high|
| transport.protocol| Transport level protocol | string | tcp| high | allowed values: **tcp**,**udp**
|port| Listening port| int| | high|
|pipeline.factory.class| Class name implementing `org.jboss.netty.channel.ChannelPipelineFactory` | class | depends on protocol| high|for tcp: `com.mckesson.kafka.connect.nettysource.DefaultTcpPipelineFactory`, for udp: `com.mckesson.kafka.connect.nettysource.DefaultUdpPipelineFactory`. See below configuration options for the factories
|ports| Listening ports| list||medium| same as '`port`' but multiply ports can be specified, used if port is already in use
|healthcheck.enabled| Enable healthcheck listener| boolean| false| medium| Enable listening tcp port for healthcheck purpose. Useful when `transport.protocol=udp`  and loadbalancer configured in front of kafka connect.
|healthcheck.bind.address| Bind address for healthcheck| string | 0.0.0.0| medium |
|healthcheck.port| Listening port for healthcheck | int | |medium|
|healthcheck.ports| Listening ports for healthcheck| list||medium| same as '`port`' but multiply ports can be specified. used if port is already in use
|ssl.enabled| Enable SSL/TLS| boolean| false|medium| can be used for `transport.protocol=tcp` only. See below for the available options

### SSL configuration options
| Name                            | Description                                                                                                                             | Type     | Default | Importance |Notes
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|----------|----------|--------------|------------|
|ssl.protocol| The SSL protocol used to generate the SSLContext.|string| TLS |
|ssl.keystore.type|
|ssl.keystore.location|
|ssl.keystore.password|
|ssl.key.alias|key alias for JKS|string|
|ssl.key.password|
|ssl.truststore.type|
|ssl.truststore.location|
|ssl.truststore.password|

>see `org.apache.kafka.common.config.SslConfigs` for more comprehensive description of above params

### PipelineFactory configuration options
| Name                            | Description                                                                                                                             | Type     | Default | Importance |Notes
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|----------|----------|--------------|------------|
|pipeline.factory.handlers| Names of handlers|list| depends on implementation | medium | List is used to add/remove/replace list of the handlers configured for PipelineFactory
|pipeline.factory.handlers.\<name\>.class| Class name of a handler | class| |medium| If empty, handler \<name\> will be removed from factory defaults, otherwise added/replaced

> ### Note:
if  ChannelHandler implements [Configurable](https://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/common/Configurable.html) properties can be added  for  each handler:
```properties
pipeline.factory.handlers=myhandler
pipeline.factory.handlers.myhandler.class=<class>
pipeline.factory.handlers.myhandler.option1=1
pipeline.factory.handlers.myhandler.option1=2
```




### DefaultTcpPipelineFactory

> class  `com.mckesson.kafka.connect.nettysource.DefaultTcpPipelineFactory`
>

#### handlers list
| Name| Class | Description
|--|--|--
|nodataTimeout| `org.jboss.netty.handler.timeout.ReadTimeoutHandler`| Raises a `ReadTimeoutException` when no data was read within a certain
|framer|`com.mckesson.kafka.connect.nettysource.DelimeterOrMaxLengthFrameDecoder`
|decoder|`org.jboss.netty.handler.codec.string.StringDecoder`
|recordHandler| `com.mckesson.kafka.connect.nettysource.StringRecordHandler`| produces SourceRecord


####  Configuration options:
| Name                            | Description                                                                                                                             | Type     | Default | Importance |Notes
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|----------|----------|--------------|------------|
|pipeline.factory.tcp.frame.maxLength|Max length of a message| int | 8192 | medium |
|pipeline.factory.tcp.frame.stripDelimiter| whether the decoded frame should strip out the delimiter or not| boolean | true | medium |
|pipeline.factory.tcp.frame.failFast| if true TooLongFrameException will be thrown immediately| boolean | false | medium | for more info see `org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder` javadoc
|pipeline.factory.tcp.frame.delimeters| delimeters to frame messages | list | \\0,\\n | medium |
|pipeline.factory.tcp.nodataTimeout | timeout value for 'nodataTimeout' channel handler|long | 0l |
|


### DefaultUdpPipelineFactory
> class  `com.mckesson.kafka.connect.nettysource.DefaultUdpPipelineFactory`
>
#### handlers list
| Name| Class | Description
|--|--|--
|framer|`com.mckesson.kafka.connect.nettysource.SinglePacketHandler`| each datagram is a message
|decoder|`org.jboss.netty.handler.codec.string.StringDecoder`
|recordHandler| `com.mckesson.kafka.connect.nettysource.StringRecordHandler`| produces SourceRecord

### HttpPipelineFactory
> class  `com.mckesson.kafka.connect.nettysource.HttpPipelineFactory`

For handling http requests

### SyslogPipelineFactory
>class  `com.mckesson.kafka.connect.nettysource.SyslogPipelineFactory`

To handle syslog messages. Produces structured record.

## Config examples
#### Simple TCP syslog/netcat connector
```properties
connector.class=com.mckesson.kafka.connect.nettysource.NettySourceConnector
tasks.max=1
topic=tcp_input
transport.protocol=tcp
port=1234
```
#### Simple UDP with healthcheck enabled
```properties
connector.class=com.mckesson.kafka.connect.nettysource.NettySourceConnector
topic=udp_input
transport.protocol=udp
ports=1234
healthcheck.enabled=true
healthcheck.ports=1234
```

#### Http input connector with enabled SSL and custom 'recordHandler'
```properties
connector.class=com.mckesson.kafka.connect.nettysource.NettySourceConnector
tasks.max=1
topic=http_input
ports=8080
transport.protocol=TCP

ssl.enabled=true
ssl.keystore.location=/path/to/jks
ssl.keystore.password=changeit
ssl.key.password=changeit
ssl.key.alias=mykey

pipeline.factory.class=com.mckesson.kafka.connect.nettysource.HttpPipelineFactory
pipeline.factory.handlers=recordHandler
pipeline.factory.handlers.recordHandler.class=com.mckesson.kafka.connect.nettysource.HttpRequestContentRecordHandler

```
