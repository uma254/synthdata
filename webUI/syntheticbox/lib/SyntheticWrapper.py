import json
import numpy as np
import pandas as pd
import random as ran

from time import time

from syndata import synthetic_analyzer as ana

from syndata.lib.utils import get_synthetic_key

import sys
import traceback

def read_metadata(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)

def get_dataset_info(file_name):
    d = ana.synthetic_analyzer(get_syndata_key())
    d.independent_analysis(file_name)
    dataset_info = {'candidate_attributes': [],
                    'categorical_attributes': [],
                    'attribute_datatypes': {},
                    'number_of_tuples': d.data_description['meta']['num_tuples'],
                    'attribute_list': d.data_description['meta']['all_attributes']}
    for attribute in d.data_description['attribute_description']:
        current_attribute_info = d.data_description['attribute_description'][attribute]
        if current_attribute_info['is_candidate_key']:
            dataset_info['candidate_attributes'].append(attribute)
        if current_attribute_info['is_categorical']:
            dataset_info['categorical_attributes'].append(attribute)
        dataset_info['attribute_datatypes'][attribute] = current_attribute_info['data_type']
    return dataset_info


def get_plot_data(input_dataset_file, synthetic_dataset_file, description_file, slicer=None, value=None):
    start_time = time()
    description = read_metadata(description_file)
    df_before = pd.read_csv(input_dataset_file)
    df_after = pd.read_csv(synthetic_dataset_file)
    plot_file_ext = "_plot.json"
    # slice data if required
    if slicer:
        print("Wrapper: generating sliced plot.......")
        print("count before: "+str(df_before.count))
        df_before = df_before[df_before[slicer]==value]
        print("count after: "+str(df_before.count))
        df_after = df_after[df_after[slicer]==value]
        plot_file_ext = "_sliced"+plot_file_ext
    plot_data = {'histogram': {}, 'barchart': {}, 'heatmap': {}}
    for attr in df_before.columns:
        if description['attribute_description'][attr]['is_categorical']:
            bins_before, counts_before = get_barchart_data(df_before[attr])
            bins_after, counts_after = get_barchart_data(df_after[attr])
            plot_data['barchart'][attr] = {'before': {'bins': bins_before, 'counts': counts_before}, 'after': {'bins': bins_after, 'counts': counts_after}}
        elif description['attribute_description'][attr]['data_type'] in {'Integer', 'Float'}:
            plot_data['histogram'][attr] = {'before': get_histogram_data(df_before[attr]), 'after': get_histogram_data(df_after[attr])}
    plot_file_name = input_dataset_file.replace(".csv", plot_file_ext)
    with open(plot_file_name, 'w') as outfile:
        json.dump(plot_data, outfile, indent=4)
    print("Plot data processed in "+str(time() - start_time)+" seconds")


def get_barchart_data(df, sort_index=True):
    distribution = df.dropna().value_counts()
    if sort_index:
        distribution.sort_index(inplace=True)
    bins = distribution.index.tolist()
    if (distribution.index.dtype == 'int64'):
        bins = [int(x) for x in distribution.index]
    return bins, distribution.tolist()    

def get_histogram_data(df):
    distribution = np.histogram(df.dropna(), bins=20)
    return [[float(distribution[1][i]), int(distribution[0][i])] for i in range(len(distribution[0]))]    


def get_syndata_key():
    return get_synthetic_key('TheSyntheticPassword','America/Caracas')

def validateDataSetCorrectness(dataFrame,columnName):
    if dataFrame.empty:
        raise EmptyDataSet()
    else:
        if columnName in dataFrame.columns:
            return
        else:
            raise MissingJoinColumnInDataSet(columnName,"Missing join column in dataset!!")

def log_exception(exception: BaseException, expected: bool = True):
    """Prints the passed BaseException to the console, including traceback.

    :param exception: The BaseException to output.
    :param expected: Determines if BaseException was expected.
    """
    output = "[{}] {}: {}".format('EXPECTED' if expected else 'UNEXPECTED', type(exception).__name__, exception)
    print(output)
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_tb(exc_traceback)

def convertFileToDataFrame(inputFileName):
    dataframe = None
    try:
        input_dataset_file = open(inputFileName, "r")
        dataframe = pd.read_csv(input_dataset_file)
     except FileNotFoundError as error:
        log_exception(error)
    except MemoryError as error:
        log_exception(error)
    finally:
        input_dataset_file.close()

    return dataframe

def get_joined_dataframe(dataframe1, dataframe2, onColumn=None,onRight = None,onLeft=None,howTo=None, timeSeriesJoin=None):
    #choosing merge command to have control over the join being done instead of join command
    if(onColumn != None):

        # validate sanity of dataframe here
        validateDataSetCorrectness(dataFrame=dataframe1,columnName=onColumn)
        validateDataSetCorrectness(dataFrame=dataframe2,columnName=onColumn)

        result = dataframe1.merge(dataframe2,on=onColumn,how=howTo)
    else:
        # validate sanity of dataframes here
        validateDataSetCorrectness(dataFrame=dataframe1, columnName=onLeft)
        validateDataSetCorrectness(dataFrame=dataframe2, columnName=onRight)

        result = dataframe1.merge(dataframe2, left_on=onLeft,right_on=onRight, how=howTo)
    print("Joined result: ")
    print(result)
    return result

def get_joined_dataframe_final(input_dataset_file, timeSeriesJoin = None, onColumn = None,onRight = None,onLeft=None, howTo = None):
    # convert input files to dataframe and then use for further processing
    i = 0
    dataframes = {}
    for eachFile in input_dataset_file:
        dataframes[i] = convertFileToDataFrame(eachFile)
        i= i+1

    i = 0
    j = 1
    result = dataframes[i]
    while(i < (len(dataframes.keys())) & j < (len(dataframes.keys()))):
        result = get_joined_dataframe(result,dataframes[j],onColumn,onLeft,onRight,howTo)
        i = i+ 1
        j = j +1

    print(result)

if __name__ == '__main__':
    filenames = {"sample1.csv","sample2.csv","sample3.csv"}
    #test with one column join
    get_joined_dataframe_final(filenames, None, 'Sl No',None,None, 'outer')
    #multiple column join
    get_joined_dataframe_final(filenames, None,None, 'Sl No','Sl No', 'outer')

class MissingJoinColumnInDataSet(Exception):
    """Exception raised for errors if columns used for join are missing in dataset.

    Attributes:
        columnName -- input columnName which caused the error
        message -- explanation of the error
    """

    def __init__(self, columnName, message="Columnname is not present in dataset"):
        self.columnName = columnName
        self.message = message
        super().__init__(self.message)

class EmptyDataSet(Exception):
    """Exception raised for errors if empty dataset is input.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self,message="Empty dataset, retry with proper dataset"):
        self.message = message
        super().__init__(self.message)

#if __name__ == '__main__':
#    print("__main__")
