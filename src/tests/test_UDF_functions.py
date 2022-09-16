from pyspark.sql import SparkSession
import os
import sys
sys.path.insert(0,'../utils')
import udf_functions as UDF
from pyspark.sql.types import StructType,StructField,StringType,FloatType,IntegerType
from pyspark.sql.functions import col, expr, concat
import importlib


def loadTest():
    " func to check functions here are loaded"
    return True

def _spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()

#------------------------- test_get_degreeUDF ---------------------------------------------

def test_get_degreeUDF(debug: bool = False, inject_error: bool = False):
    """ 
        test utils.udf_functions.get_degreeUDF 

        :param debug: prints DF test outputs and returns test data DF and results DF
                      If Debug is NOT set, then this test will either PASS or FAIL (assertion error)
 
    """
    importlib.reload(UDF)
    spark = _spark_session()

    #header = ["Minimum Qual Requirements","ExpectedDegree"]
    requirements_schema = StructType([       
        StructField('Minimum Qual Requirements', StringType(), False),
        StructField('ExpectedDegree', StringType(), False),
    ])
    requirements = [
        ("Associate","Associate"),
        ("blar Associate blar","Associate"),
        ("blar associate blar","Associate"),
        ("A baccalaureate degree blar blar An associate degree","Associate"),
        ("blar A baccalaureate degree from an accredited ","Degree"),
        ("blar blar Bachelors degree needed","Degree"),
        ("blar blar BSc needed","Degree"),
        ("word boundary test BScXXXX needed","none"),
        ("word boundary test  YYYBSc needed","none"),
        ("word boundary test  YYYBScZZZ needed","none"),
        ("blar blar BA needed","Degree"),
        ("word boundary test BAXXXX needed","none"),
        ("word boundary test YYYBA needed","none"),
        ("word boundary test YYYBAZZZ needed","none"),
        (" least a bachelors degree but masters preferred","Degree"),
        ("blar blar A master's degree from an accredited college in","Masters"),
        ("blar blar A masters degree from an accredited college in","Masters"),
        ("word boundary test MScXXX degreee","none"),
        ("word boundary test ZZZMSc degreee","none"),
        ("word boundary test XXXMScXXX degreee","none"),
        ("blar blar Doctoral degree needed","PhD"),
        ("blar blar doctorate degree needed","PhD"),
        ("blar blar PhD needed","PhD"),
        ("blar blar phd needed","PhD"),
        ("boundary test XXXPhD needed","none"),
        ("boundary test PhDYYY needed","none"),
        ("boundary test XXXPhDXXX needed","none"),
    ]

    requirements_df = spark.createDataFrame(data = requirements, schema = requirements_schema)
    if debug:
        print(f"requirements_df\n{requirements_df.show()}")

    # creates Degree column based on 'Minimum Qual Requirements'
    requirements_tested_df = requirements_df.\
                                withColumn("Degree",
                                    UDF.get_degreeUDF(
                                            col('Minimum Qual Requirements')
                                    ))
    if debug:
        print(f"requirements_tested_df\n{requirements_tested_df.show()}")

    requirements_results_df = requirements_tested_df.\
                                    withColumn("Test Result",
                                        expr("""
                                            case
                                                when
                                                    Degree == ExpectedDegree
                                                    then 'PASS'
                                                else
                                                    concat('FAIL ',
                                                        'Degree ',
                                                        Degree,
                                                        ' != ',
                                                        'ExpectedDegree ',
                                                        ExpectedDegree)
                                            end
                                        """))
    if debug:
        print(f"requirements_results_df\n{requirements_results_df.show()}")

    unit_test_passed = True
    results = requirements_results_df.collect()
    for res in results:
        if res["Test Result"] == "PASS":
            pass 
        else:
            unit_test_passed = False
            break

    if unit_test_passed:
        if debug:
            return requirements_df,requirements_results_df
    else:
        if debug:
            return requirements_df,requirements_results_df
        else:
            raise Exception("One or more tests failed, i.e. Higher Degree != ExpectedDegree, run again with debug for more details")


#------------------------------- test_get_HourlySalaryUDF -------------------------------------


def test_get_HourlySalaryUDF(debug: bool = False, inject_error: bool = False):
    """ 
        test utils.udf_functions.get_HourlySalaryUDF 

        :param debug: prints DF test outputs and returns test data DF and results DF
                      If Debug is NOT set, then this test will either PASS or FAIL (assertion error)
        :param inject_error: to test the test.  Add a dileberate error!  
 
    """
    importlib.reload(UDF)
    spark = _spark_session()

    header= ["SalaryRangeFrom","SalaryRangeTo","SalaryFrequency","ExpectedHourlySalary"]
    #salary_schema = StructType([       
    #    StructField('SalaryRangeFrom', FloatType(), True),
    #    StructField('SalaryRangeTo', FloatType(), True),
    #    StructField('SalaryFrequency', StringType(), True),
    #    StructField('ExpectedHourlySalary', FloatType(), False),
    #])

    salary_data = [
         (0.0,100000.0,"Annual",56.92)
        ,(None,100000.0,"Annual",56.92)
        ,(100000.0,None,"Annual",56.92)
        ,(80000.0,100000.0,"Annual",51.22)
        ,(100000.0,80000.0,"Annual",51.22)
        ,(None,None,"Annual",None)
        ,(None,10.0,"Hourly",10.0)
        ,(0.0,10.0,"Hourly",10.0)
        ,(5.0,10.0,"Hourly",7.5)
        ,(None,80.0,"Daily",10.0)
        ,(0.0,80.0,"Daily",10.0)
        ,(60.0,80.0,"Daily",8.75)
    ]
    if inject_error:
        err = (60.0,80.0,"Daily",8.0)
        salary_data.append(err)

    salary_data_df = spark.createDataFrame(data = salary_data, schema = header)

    salary_data_df_tested = salary_data_df.\
                                withColumn("HourlySalary",
                                    UDF.get_HourlySalaryUDF(
                                            col('SalaryRangeFrom'),
                                            col('SalaryRangeTo'),
                                            col('SalaryFrequency')
                                    ))
    #
    # An explanation of the somewhat strange use of teh extra column TestDiff:
    # This was my last stop fix to try to deal with the rounding errors I was getting
    # where I have been unable to set 2 dec places for HourlySalary in the UDF
    # So instead I rounded HourlySalary and ExpectedHourlySalary to 2 dec places and made the 
    # comparison in a case statment.  Even thoughthe number looked correct my comparrison test 
    # failed.
    # Summary: I decided to be pragmatic by diff-ing HourlySalary and ExpectedHourlySalary
    #          if the TestDiff is less than 1 cent, then the values are equal.
    # NOTE to reviewer: please show me how to fix this in less of a hacky way!  *thanks*
    #
    salary_data_df_results = salary_data_df_tested.\
                                    withColumn('TestDiff',
                                                col('HourlySalary') - col('ExpectedHourlySalary')).\
                                    withColumn("Test Result",
                                            expr("""
                                                case
                                                    when
                                                        HourlySalary is NULL and ExpectedHourlySalary is NULL
                                                        then 'PASS'
                                                    when 
                                                        TestDiff < 0.001
                                                        then 'PASS'
                                                    else
                                                        concat('FAIL ',
                                                                'HourlySalary ',
                                                                round(HourlySalary,2),
                                                                ' != ',
                                                                'ExpectedHourlySalary ',
                                                                round(ExpectedHourlySalary,2))
                                                end
                                            """))
    
    
    if debug:
        print(f"salary_data_df\n{salary_data_df.show()}")
        print(f"salary_data_df_results\n{salary_data_df_results.show()}")

    unit_test_passed = True
    results = salary_data_df_results.collect()
    for res in results:
        if res["Test Result"] == "PASS":
            pass 
        else:
            unit_test_passed = False
            break


    if unit_test_passed:
        if debug:
            return salary_data_df,salary_data_df_results
    else:
        if debug:
            return salary_data_df,salary_data_df_results
        else:
            raise Exception("One or more tests failed, i.e. ExpectedHourlySalary != HourlySalary, run again with debug for more details")


# -------------------------------------
# print Loaded when module is loaded or reloaded
print("Loaded")