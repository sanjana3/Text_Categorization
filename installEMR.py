import boto3region_name='region_name'def get_security_group_id(group_name, region_name):
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']emr = boto3.client('emr', region_name=region_name)cluster_response = emr.run_job_flow(
        Name='cluster_name', # update the value
        ReleaseLabel='emr-5.27.0',
        LogUri='s3_path_for_logs', # update the value
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.2xlarge', # change according to the requirement
                    'InstanceCount': 1 #for master node High Availabiltiy, set count more than 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.2xlarge', # change according to the requirement
                    'InstanceCount': 2
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'key_pair_name', # update the value
            'EmrManagedMasterSecurityGroup': get_security_group_id('ElasticMapReduce-master', region_name=region_name)
            'EmrManagedSlaveSecurityGroup': get_security_group_id('ElasticMapReduce-master', region_name=region_name)
        },
        BootstrapActions=[    {
                    'Name':'install_dependencies',
                    'ScriptBootstrapAction':{
                            'Args':[],
                            'Path':'path_to_bootstrapaction_on_s3' # update the value
                            }
                }],
        Steps = [],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Applications=[
            { 'Name': 'hadoop' },
            { 'Name': 'spark' },
            { 'Name': 'hive' },
            { 'Name': 'zeppelin' },
            { 'Name': 'presto' }
        ],
        Configurations=[
            # YARN
            {
                "Classification": "yarn-site", 
                "Properties": {"yarn.nodemanager.vmem-pmem-ratio": "4",
                               "yarn.nodemanager.pmem-check-enabled": "false",
                               "yarn.nodemanager.vmem-check-enabled": "false"}
            },
            
            # HADOOP
            {
                "Classification": "hadoop-env", 
                "Configurations": [
                        {
                            "Classification": "export", 
                            "Configurations": [], 
                            "Properties": {"JAVA_HOME": "/usr/lib/jvm/java-1.8.0"}
                        }
                    ], 
                "Properties": {}
            },
            
            # SPARK
            {
                "Classification": "spark-env", 
                "Configurations": [
                        {
                            "Classification": "export", 
                            "Configurations": [], 
                            "Properties": {"PYSPARK_PYTHON":"/usr/bin/python3",
                                           "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"}
                        }
                    ], 
                "Properties": {}
            },
            {
                "Classification": "spark",
                "Properties": {"maximizeResourceAllocation": "true"},
                "Configurations": []
             },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.dynamicAllocation.enabled": "true" #default is also true
                }
            }
        ]
    )