#程序配置变更记录
2019-03-05
1.增加报警邮件配置，主要修改报警邮件的收件人

	# 接收者列表，通过","分割
	email.recipient=zhucg@bonree.com

2.增加磁盘监控 邮件报警，主要修改以下参数，这个根据运维的需求进行修改

	# 资源报警邮件发送时间间隔控制 单位s
	resource.email.time = 300

	#资源磁盘剩余告警值，当分区剩余大小低于该值，则会发送邮件告警，单位KB,
	limit.resource.value.disk.remain.size=20971520

3.修改资源选择策略，增加限定值，若超过该值则不参与写入服务,主要修改以下参数。默认为10G

	#资源磁盘剩余限制值，当分区剩余量低于该值，则不会参与写入服务选择， 单位kb
	limit.resource.value.force.disk.remain.size=10485760

4.增加deliver推送zeus机制，增加参数如下：
    #deliver的开关，false为关闭，不进行推送
    deliver.switch=false
    #kafka的brokers的地址列表
    kafka.brokers=192.168.4.114:9092
    #deliver推送的kafka的topic
    kafka.topic=brfs_metric
    #deliver缓存大小，超过大小后，将丢弃数据
    deliver.queue.size=200000
    #deliver解析字段地址
    deliver.meta.url=http://devtest.ibr.cc:20003/v1
    #deliver数据源
    deliver.datasource=sdk_data_brfs
    #deliver的写性能表
    deliver.table.writer=brfs_writer_metric
    #deliver的读性能表
    deliver.table.reader=brfs_reader_metric
5.增加过滤监控磁盘分区配置
  # 不监控的磁盘分区，若brfs数据存储路径上存在多个磁盘分区时，可通过该配置项过滤掉不想监控的磁盘分区
  # 例如brfs数据存储目录为/data/files 其中/目录挂在一个分区，/data目录挂载一个分区，不想监控/目录对应的分区则可以将/放入该配置项中，若存在多个，用，分割。 默认为空
  # unmounitor.partition = /