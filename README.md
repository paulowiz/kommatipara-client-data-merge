
  
<!-- PROJECT -->  
<p align="center">  
  <h3 align="center">   
   Programming Exercise using PySpark  
  </h3>   
</p>  
  
<!-- ABOUT THE PROJECT -->  
## 🤔 Introduction  
A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

  
<br />   
  
  
<!-- INSTALLATION -->  
  

> If you are on Linux use `python3 pythonfile.py` to run the application
> and to install new libraries use `pip3 install libraryname`

 
  
## 🔨 Installation and Running  
  
1. Clone this repository  
  
2. Install the requirements running  `pip install -r requirements.txt`  
  
3. Set SPARK_MASTER_URL as enviroment variable with your spark://youurl:port(spark://localhost:7077) otherwise the program will setup `local[*]` as your master url.

4. Run the following command in the `python main.py --path1 "dataset_one.csv" --path2 "dataset_two.csv" --countries "Netherlands,United Kingdom` directory to run the action. 

Extra Steps

I've tried to implement docker with apache spark server and you can follow also these steps to use apache spark on docker:

1. Run the command to download and run the docker-compose containers `docker-compose up -d --build`

2. Your Apache Spark will be running at  `http://localhost:8080` and your MASTER URL to setup on SPARK_MASTER_URL is `spark://localhost:7077`

3.  Run the following command in the `python main.py --path1 "dataset_one.csv" --path2 "dataset_two.csv" --countries "Netherlands,United Kingdom` directory to run the action. 

<br />  
  
<!-- File Overview -->  
  
## 📚 Project files overview
  
- **main.py** main file to run and call the ETL process.
- **requirements.txt**  Dependencies from the project
- **spark_utils.py** Class with generic functions to use with pyspark.
- **test_spark_utils.py** Test file to use with pytest to test our generic functions.
- **dataset_one.csv** Dataset with client information
- **dataset_two.csv** Dataset with client's financial information.
- **.gitignore**  Define files that should be ignored by GIT.
- **docker-compose.yml**  Docker file to run Apache Spark container and its worker.

## 🔓Author and Acknowledgements

Author - [Paulo Mota](https://www.linkedin.com/in/paulo-mota-955218a2/)<br>
Testing library: [Chispa](https://github.com/MrPowers/chispa)<br>
