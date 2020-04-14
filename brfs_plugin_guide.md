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