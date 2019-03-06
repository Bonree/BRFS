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