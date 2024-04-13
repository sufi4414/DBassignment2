The Files inside student_files folder are compatible to run in google collab.<br>
The Files outside (q1.py to q2.py ) are compatible to run in hadoop.<br>
HW2_GoogleCollab.ipynb file is the one to run in collab.<br><br>

Localhost private ip: 172.31.89.26<br>
Localhost public ip (incase you want to SSH): 44.201.136.19<br>
Cluster name: hw2_cluster<br>
My local path for aws pem file: /Users/sufi/aws/aws_edu.pem<br><br>

To start hadoop and spark:<br>
/home/ec2-user/hadoop/sbin/start-all.sh<br>
/home/ec2-user/spark/sbin/start-all.sh<br><br>

To stop hadoop and spark:<br>
/home/ec2-user/hadoop/sbin/stop-all.sh<br>
/home/ec2-user/spark/sbin/stop-all.sh<br><br>

Preliminary Check: Check if NameNode is running. Use jps command to check.<br><br>

To SSH:<br>
ssh -i /path/to/aws_pem file ec2-user@44.201.136.19<br><br>

