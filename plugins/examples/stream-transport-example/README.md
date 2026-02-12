# Stream Transport Example

Example plugin demonstrating streaming transport actions in OpenSearch.

## Overview

This plugin shows two patterns for streaming transport actions:

1. **Basic Streaming Action** - Extends `StreamTransportAction` (similar to `TransportAction`)
2. **Nodes Streaming Action** - Extends `TransportStreamNodesAction` (similar to `TransportNodesAction`)

## Client Usage

```java
client.executeStream(
    MyStreamAction.INSTANCE,
    new MyRequest(),
    new StreamActionListener<MyResponse>() {
        public void onStreamResponse(MyResponse response) { /* handle each response */ }
        public void onFinalResponse() { /* stream complete */ }
        public void onStreamFailure(Exception e) { /* handle error */ }
    }
);
```

## Basic Streaming Action

Extends `StreamTransportAction` for actions that stream multiple responses.

### 1. Define Action
```java
public class MyStreamAction extends ActionType<MyResponse> {
    public static final MyStreamAction INSTANCE = new MyStreamAction();
    public static final String NAME = "cluster:admin/my_stream";
    private MyStreamAction() { super(NAME, MyResponse::new); }
}
```

### 2. Implement Transport Action
```java
public class TransportMyStreamAction extends StreamTransportAction<MyRequest, MyResponse> {
    private final StreamTransportService streamTransportService;
    private final TransportService transportService;

    @Inject
    public TransportMyStreamAction(
        StreamTransportService streamTransportService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(MyStreamAction.NAME, streamTransportService, actionFilters);
        this.streamTransportService = streamTransportService;
        this.transportService = transportService;

        streamTransportService.registerRequestHandler(
            MyStreamAction.NAME,
            ThreadPool.Names.GENERIC,
            MyRequest::new,
            this::handleRequest
        );
    }

    @Override
    public void executeStream(MyRequest request, StreamActionListener<MyResponse> listener) {
        streamTransportService.sendRequest(
            transportService.getLocalNode(),
            MyStreamAction.NAME,
            request,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new StreamTransportResponseHandler<MyResponse>() {
                public void handleStreamResponse(StreamTransportResponse<MyResponse> stream) {
                    try {
                        MyResponse response;
                        while ((response = stream.nextResponse()) != null) {
                            listener.onStreamResponse(response);
                        }
                        stream.close();
                        listener.onFinalResponse();
                    } catch (Exception e) {
                        stream.cancel("Error", e);
                        listener.onStreamFailure(e);
                    }
                }

                public void handleException(TransportException e) {
                    listener.onStreamFailure(e);
                }

                public String executor() { return ThreadPool.Names.SAME; }

                public MyResponse read(StreamInput in) throws IOException {
                    return new MyResponse(in);
                }
            }
        );
    }

    private void handleRequest(MyRequest request, TransportChannel channel, Task task) {
        try {
            // Send multiple batched responses
            channel.sendResponseBatch(new MyResponse(...));
            channel.sendResponseBatch(new MyResponse(...));
            channel.completeStream();
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }
}
```

### 3. Register Action
```java
public class MyPlugin extends Plugin implements ActionPlugin {
    public List<ActionHandler<?, ?>> getActions() {
        return List.of(new ActionHandler<>(MyStreamAction.INSTANCE, TransportMyStreamAction.class));
    }
}
```

## Nodes Streaming Action

Extends `TransportStreamNodesAction` for actions that stream responses from nodes (like `TransportNodesAction`).

### 1. Define Action
```java
public class MyNodesStreamAction extends ActionType<MyNodesResponse> {
    public static final MyNodesStreamAction INSTANCE = new MyNodesStreamAction();
    public static final String NAME = "cluster:admin/my_nodes_stream";
    private MyNodesStreamAction() { super(NAME, MyNodesResponse::new); }
}
```

### 2. Implement Transport Nodes Action
```java
public class TransportMyNodesStreamAction extends TransportStreamNodesAction<
    MyNodesRequest,
    MyNodesResponse,
    MyNodeRequest,
    MyNodeResponse> {

    @Inject
    public TransportMyNodesStreamAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        StreamTransportService streamTransportService,
        ActionFilters actionFilters
    ) {
        super(
            MyNodesStreamAction.NAME,
            threadPool,
            clusterService,
            streamTransportService,
            actionFilters,
            MyNodesRequest::new,
            MyNodeRequest::new,
            ThreadPool.Names.GENERIC,
            MyNodeResponse.class
        );
    }

    @Override
    protected void nodeStreamOperation(MyNodeRequest request, TransportChannel channel, Task task) {
        try {
            // Send multiple batched responses from this node
            channel.sendResponseBatch(new MyNodeResponse(...));
            channel.sendResponseBatch(new MyNodeResponse(...));
            channel.completeStream();
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }

    @Override
    protected MyNodeRequest newNodeRequest(MyNodesRequest request) {
        return new MyNodeRequest(request);
    }

    @Override
    protected MyNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new MyNodeResponse(in);
    }

    @Override
    protected MyNodesResponse newResponse(
        MyNodesRequest request,
        List<MyNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new MyNodesResponse(clusterService.getClusterName(), responses, failures);
    }
}
```

### 3. Register Action
```java
public class MyPlugin extends Plugin implements ActionPlugin {
    public List<ActionHandler<?, ?>> getActions() {
        return List.of(new ActionHandler<>(MyNodesStreamAction.INSTANCE, TransportMyNodesStreamAction.class));
    }
}
```

## Key Points

- **Server**: Always call `channel.completeStream()` when done, or `channel.sendResponse(exception)` on error
- **Client**: Always call `stream.close()` on success, or `stream.cancel()` on error
- **Nodes Action**: Base class handles node iteration and coordination, you only implement `nodeStreamOperation()`
