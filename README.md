# Input files
I created a bucket in my personal AWS account where I uploaded the sample data.
I have also configured the bucket to be accesed by a particular user, when running in EMR
this is usually not required (Instance Role).

# Tools
I decided to use PySpark for scalability reasons. Obviously, I just run it in local mode
using just my machine to avoid costs of spinning up EMR clusters.

# External packages
I used "tldextract" for making eaiser the domain extraction. My logic has been:
'Why to create custom regex when there is a package out there that is already tested?'
Similar thinking when extracting the youtube video id.

# Dataset
I found two columns, in the dataset, that are mentioned in the requirements, I have
named them 'UNKNOWN1' and 'UNKNOWN2'.

# Other considerations
When saving the processed dataframe I did not drop the old columns, but drop columns is as easy
as calling the drop method.
Some of the requirements are covered by Spark itself, passing the right schema and the right separator as well 
as the right NULL value.

# How to run
To run just call the main file passing the bucket where the files are stored as argument:
```bash
python3 runner/main.py bucketName/
```

# Tests
The tests are inside the 'test' directory

# Environment
The application has been tested under the following environment:
```bash
Python 3.6.7 (default, Oct 22 2018, 11:32:17) 
[GCC 8.2.0] on linux

Linux 4.15.0-45-generic #48-Ubuntu SMP Tue Jan 29 17:28:13 UTC 2019 x86_64 x86_64 x86_64 GNU/Linux
```