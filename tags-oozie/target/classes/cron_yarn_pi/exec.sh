# У�������ļ�
/export/servers/oozie/bin/oozie validate -oozie http://bigdata-cdh01.itcast.cn:11000/oozie/ /root/cron_yarn_pi/coordinator.xml

# ����job
/export/servers/oozie/bin/oozie job -oozie http://bigdata-cdh01.itcast.cn:11000/oozie/ -config /root/cron_yarn_pi/job.properties -run