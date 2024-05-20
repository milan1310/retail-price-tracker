Configration_Type= "Local"   
# Configration_Type= "Server"


if Configration_Type == "Local":
    user = "postgres"
    password = "first"
    host = "localhost"
    port = "5432"
    database = "postgres"
    redispassword="your redis password"
    aws_access_key_id=""
    aws_secret_access_key_id=""
    source_email= ""
    base_url="http://localhost:5000"
    s3_bucket_name='mobil-aws'
    local_path='/mnt/d/mobil/src/mobil'
    # source_email= "milan@mediaamp.co.in"
    
    
elif Configration_Type == "Server":
    pass
    #same as above but with server details
    
