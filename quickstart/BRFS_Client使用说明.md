# BRFS客户端maven地址：
```xml
<dependency>
    <groupId>com.bonree.fs</groupId>
    <artifactId>brfs-client</artifactId>
    <version>2.0.8</version>
</dependency>
```

# 客户端创建

```java
// RegionNode的地址指定一个就可以正常工作，也可以指定多个RegionNode的地址信息增加容错性
URI[] regionNodeUrls = new URL[] {URI.create("http://localhost:8200")};
// 设置客户端的配置参数
ClientConfiguration configuration = new ClientConfigurationBuilder().build();

// 构建BRFS接口实例。
// 如果有用户名及密码
BRFS client = new BRFSClientBuilder()
            .config(configuration)
            .build(userName, passwd, regionNodeUrls);

// 如果没有用户名及密码
BRFS client = new BRFSClientBuilder()
            .config(configuration)
            .build(regionNodeUrls);

// 调用相关接口
// to do something

// 关闭客户端
client.shutdown();
```

客户端的配置参数通过`ClientConfigurationBuilder`构建，可以设置的参数为：

- **data package size**

  单个文件传输时，每次向服务端发送的数据包大小。例如一个32K的文件会被切分为两个16K的包进行发送。批量写入不受这个参数的影响。默认值为16K。

- **connect timeout**

  与服务端建立网络连接的超时时间。默认10s。

- **request timeout**

  处理一个请求的超时时间，也即从发送请求开始到处理响应的整个过程允许的最大时长。默认30s。

- **read timeout**

  从网络连接中读取数据的超时时间。默认为30s。

- **write timeout**

  向网络连接写入数据的超时时间。默认为30s。

- **discovery expiration time**

  discovery模块负责获取BRFS中的节点信息，并缓存在客户端内存中。这个参数设置缓存数据的过期时间，过期的数据会被清除。默认30s。

- **discovery refresh interval**

  discovery模块中缓存信息的刷新时间间隔。缓存数据的刷新是一个延迟操作，只有在被访问的情况下才会进行。换句话说，当数据被添加到缓存后，刷新间隔内访问数据都会直接返回缓存数据，当超过刷新间隔后，如果数据被再次访问，则缓存数据会被刷新；如果没有访问，则缓存中会一直保留旧值，直到数据过期被清除缓存。默认为5s。



# 接口说明

BRFS客户端中的接口分为4类接口：

- StorageRegion管理接口
- 文件写入接口
- 文件读取接口
- 文件管理接口



## StorageRegion管理接口

1. 创建StorageRegion

   ```java
   StorageRegionID createStorageRegion(CreateStorageRegionRequest request) throws Exception;
   ```

   ​	srName规范按照: ```T_产品名称_业务名称```定义，不符合格式要求不予创建

   CreateStorageRegionRequest`通过`CreateStorageRegionRequestBuilder`进行构建，可以设置如下属性：

   - ReplicateNum

     StorageRegion中数据的副本数。

   - FileCapacity

     StorageRegion中文件块的字节大小。

   - FilePartition

     文件存储的时间间隔。

   - DataTTL

     数据过期时间

   - Enabled

     StorageRegion是否可用。被禁用的StorageRegion不能写入数据。



2. 更新StorageRegion

   ```java
   boolean updateStorageRegion(String srName, UpdateStorageRegionRequest request) throws Exception;
   ```



3. 删除StorageRegion

   ```java
   void deleteStorageRegion(String srName) throws Exception;
   ```

   StorageRegion在被删除前必须设置Enabled属性为false。



4. 获取StorageRegion信息

   ```java
   StorageRegionInfo getStorageRegionInfo(String srName) throws Exception;
   ```



5. 判断StorageRegion是否存在

   ```java
   boolean doesStorageRegionExists(String srName) throws Exception;
   ```



6. 获取StorageRegion列表

   ```java
   List<String> listStorageRegions() throws Exception;
   ```

   获取所有的StorageRegion列表。

   ```java
   List<String> listStorageRegions(ListStorageRegionRequest request) throws Exception;
   ```

   获取符合条件的StorageRegion列表。



## 数据写入接口

1. 单文件写入接口

   ```java
   // 不带自定义文件名的写入接口
   PutObjectResult putObject(String srName, byte[] bytes) throws Exception;
   PutObjectResult putObject(String srName, File file) throws Exception;
   PutObjectResult putObject(String srName, InputStream input) throws Exception;

   //通过BRFSPath设置自定义文件名的写入接口
   PutObjectResult putObject(String srName, BRFSPath objectPath, byte[] bytes) throws Exception;
   PutObjectResult putObject(String srName, BRFSPath objectPath, File file) throws Exception;
   PutObjectResult putObject(String srName, BRFSPath objectPath, InputStream input) throws Exception;
   ```



2. 批量写入接口

   ```java
   BatchResult putObjects(String srName, PutObjectBatch batch) throws Exception;
   ```

   `PutObjectBatch`中可以声明多个文件，每个文件可以独立的决定是否使用自定义文件名。返回结果包含相同数据的FID信息。如果某个文件写入失败，则其FID信息为null。



## 数据读取接口

1. 获取单个文件的数据

   ```java
   // 文件内容作为BRFSObject对象返回
   BRFSObject getObject(GetObjectRequest request) throws Exception;
   // 文件内容直接写入outpuFile中
   void getObject(GetObjectRequest request, File outputFile) throws Exception;
   // 异步获取文件内容，结果写入outputFile文件，通过future指示操作结果
   ListenableFuture<?> getObject(GetObjectRequest request, File outputFile, Executor executor);
   ```

   `GetObjectRequest`中可以设置FID，也可以设置自定义文件路径。



2. 获取用户自定目录下的所有文件数据

   ```java
   List<BRFSObject> getObjects(GetObjectsRequest request) throws Exception;
   ```

   `GetObjectsRequest`中只能设置自定义目录，接口返回该目录下所有的文件内容，每个文件作为一个`BRFSObject`返回。



## 文件管理接口

1. 判断文件是否存在

   ```java
   boolean doesObjectExists(String srName, BRFSPath path) throws Exception;
   ```

   目前只提供了判断自定义文件路径是否存在的接口。



2. 删除文件

   ```java
   // 文件删除只能通过指定时间区间的方式完成
   void deleteObjects(String srName, long startTime, long endTime) throws Exception;
   ```



## 客户端的第三方依赖包版本

```
com.google.guava:guava:28.2-jre
com.fasterxml.jackson.core:jackson-annotations:2.9.8
com.fasterxml.jackson.core:jackson-core:2.9.8
com.fasterxml.jackson.core:jackson-databind:2.9.8
com.squareup.okhttp3:okhttp:3.14.7
com.squareup.okio:okio:1.17.2
org.apache.commons:commons-pool2:2.8.0
org.slf4j:slf4j-api:1.7.30
com.google.protobuf:protobuf-java:3.4.0
```

