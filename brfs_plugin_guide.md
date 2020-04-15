构建brfs插件流程说明：

1、新建一个继承于com.bonree.fs:BRFS的maven模块；

2、pom.xml中添加：

``<build>`
		`<plugins>`
			`<plugin>`
				`<artifactId>maven-assembly-plugin</artifactId>`
				`<executions>`
					`<execution>`
						`<id>plugin-assembly</id>`
						`<phase>package</phase>`
						`<goals>`
							`<goal>single</goal>`
						`</goals>`
						`<configuration>`
							`<appendAssemblyId>false</appendAssemblyId>`
							`<descriptorRefs>`
								`<descriptorRef>brfs-plugin</descriptorRef>`
							`</descriptorRefs>`
						`</configuration>`
					`</execution>`
				`</executions>`
			`</plugin>`
		`</plugins>`
	</build>`

这个maven插件的目的是对brfs插件的jar包及其依赖进行zip打包。

3、引入与brfs服务共用的注解类的依赖时，比如用于注入的javax.inject:javax.inject和jaxrs注解依赖javax.ws.rs:javax.ws.rs-api时，需要设置作用域为provided。如果是插件内部使用的注解可以没有这个限制。

4、在src/main/resources路径下添加service资源文件：

*META-INF/services/com.bonree.brfs.common.plugin.BrfsModule*

并添加相应的实现类的完整名称作为文件内容，如果有多个实现类，则每个实现类名占一行。

5、如果需要让插件和brfs一起打包发布，需要在brfs-distribution工程的pom.xml文件中添加对插件的依赖，其中依赖的type为zip，scope为provided，例如：

​        ``<dependency>`
​            `<groupId>com.bonree.fs</groupId>`
​            `<artifactId>brfs-http-netty</artifactId>`
​            `<version>${project.version}</version>`
​            `<type>zip</type>`
​            `<scope>provided</scope>`
​        </dependency>`

然后在build-tools工程下的*src/main/resources/assemblies/brfs-server.xml*文件中的fileSets标签中添加如下内容：

```xml
<fileSet>
  <directory>${project.build.directory}/dependency/${name}-${project.version}</directory>
  <outputDirectory>plugins/${name}</outputDirectory>
</fileSet>
```
把${name}替换为相应的插件名即可。