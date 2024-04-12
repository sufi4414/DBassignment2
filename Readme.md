The Files inside student_files folder are compatible to run in google collab.<br>
The Files outside (q1.py to q2.py ) are compatible to run in hadoop.
Untitled0.ipynb file is the one to run in collab.

Localhost private ip: 172.31.89.26
Localhost public ip (incase you want to SSH): 44.201.136.19
Cluster name: hw2_cluster
My local path for aws pem file: /Users/sufi/aws/aws_edu.pem

To start hadoop and spark:
/home/ec2-user/hadoop/sbin/start-all.sh
/home/ec2-user/spark/sbin/start-all.sh

To stop hadoop and spark:
/home/ec2-user/hadoop/sbin/stop-all.sh
/home/ec2-user/spark/sbin/stop-all.sh

Preliminary Check: Check if NameNode is running. Use jps command to check.

To SSH:
ssh -i /path/to/aws_pem file ec2-user@44.201.136.19

