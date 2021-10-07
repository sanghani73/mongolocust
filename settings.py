# Database name to use
DB_NAME = 'fiservTest'

# cluster connection string
CLUSTER_URL = f'mongodb+srv://user:password@something.mongodb.net/{DB_NAME}?retryWrites=true&w=majority'

# Base data loading parameters
# Grouped processing
GROUP_1_PROCESSES = 1
GROUP_2_PROCESSES = 3
GROUP_3_PROCESSES = 10

# Weighted processing
PROCESSES = 10
