# У�������ļ�
/export/servers/oozie/bin/oozie validate -oozie http://bigdata-cdh01.itcast.cn:11000/oozie/ /root/wf_spark_pi/workflow.xml

# ����job
/export/servers/oozie/bin/oozie job -oozie http://bigdata-cdh01.itcast.cn:11000/oozie/ -config /root/wf_spark_pi/job.properties -run