"""
Harvard Library Historical Datasets Curation Functions Module

Functions supporting the processing of files associated with the Trade Statistics volume,
dataset creation, and upload of datafiles. 

Intended to demonstrate pilot of Historic Datasets curation strategy that creates one dataset 
per table series.
"""
import json
import numpy as np
import pandas as pd
from pyDataverse.models import Dataset
import requests
import dvuploader as dv

def create_dataset_metadata(author, affiliation, contact, email, series_name, series_inventory):
    """
    Create a dictionary of dataset metadata

    Parameters
    ----------
    author : str
        Dataset author name
    affiliation : str
        Dataset athor affiliation
    contact : str
        Dataset contact name (may be same as author)
    email : str
        Dataset contact email address
    series_name : str
        Name of series (e.g., Tonnage: 1)
    series_inventory : DataFrame
        DataFrame containing file metadata

    Return
    ------
    dict
    """
    # validate parameters
    if ((not author) or
        (not affiliation) or
        (not contact) or
        (not email) or
        (not series_name) or 
        (series_inventory.empty == True)):
            print('Error: One or more invalid parameter values')
            return {}
    
    # check the inventory for required fields
    if ((not 'series_name' in series_inventory.columns) or
        (not 'volume_title' in series_inventory.columns) or
        #(not 'attribution' in series_inventory.columns) or
        (not 'author' in series_inventory.columns) or
        (not 'subjects' in series_inventory.columns) or 
        (not 'creation_date' in series_inventory.columns) or 
        (not 'url' in series_inventory.columns)):
            print('Error: One or more missing required fields in inventory')
            return {}

    # get the index of the first file in the inventory
    index = series_inventory.index.values[0]
    
    # collect metadata variables
    dataset_title = series_inventory.at[index,'series_name']
    volume_title = series_inventory.at[index,'volume_title']
    #volume_attribution = series_inventory.at[index,'attribution']
    volume_author = series_inventory.at[index,'author']
    creation_date = series_inventory.at[index,'creation_date']
    
    keyword_str = series_inventory.at[index,'subjects']
    keywords = keyword_str.split(';')
    kws = []
    for kw in keywords:
        d = {}
        d['keywordValue'] = kw
        d['keywordVocabulary'] = 'LCSH'
        d['keywordVocabularyURI'] = 'https://www.loc.gov/aba/cataloging/subject/'
        kws.append(d)
    #topic class
    topic_class_str = series_inventory.at[index,'topic_class']
    topics = topic_class_str.split(';')
    tps = []
    for top in topics:
        t = {}
        t['topicClassValue'] = top
        tps.append(t)
    
    creation_date = '{}-01-01'.format(series_inventory.at[index,'creation_date'])
    hollis_link = series_inventory.at[index,'permalink']
    drs_link = series_inventory.at[index,'url']
    subjects = ['Social Sciences']
    data_source = [series_inventory.at[index,'url']]
    description = '{} is a series of tables and text files associated with: {}. Created by: {}'.format(dataset_title,
                                                                                                       volume_title,
                                                                                                       volume_author)

    # build the dataset metadata dictionary
    dataset_metadata = {
        'title':dataset_title,
        'author': [{'authorName':author,'authorAffiliation':affiliation}],
        'description':[{'dsDescriptionValue':description}],
        'contact':[{'datasetContactName':contact,
                                        'datasetContactAffiliation':affiliation,
                                        'datasetContactEmail':email}],
        'subject':['Social Sciences'],
        'origin_of_sources':'<a href=\"{}\">{}</a>'.format(hollis_link, volume_title),
        'license':'CC0 1.0',
        'keywords':kws,
        'topic_classification':tps,
        'data_source':data_source,
        'creation_date':creation_date
    }

    return dataset_metadata

def create_dataset(api, dataverse_url, dataset_metadata):
    """
    Create a dataverse dataset

    Parameters
    ----------
    api : pyDataverse API
    dataverse : str
        Name of dataverse collection url (e.g., https://demo.dataverse.org/dataverse/histd)
    dataset_metadata : dict
        Dictionary of dataset metadata values

    Return
    ------
    dict: 
        {status: bool, dataset_id: int, dataset_pid: str}

    """
    # validate parameters
    if ((not api) or
        (not dataset_metadata)):
        return {
            'status':False, 
            'dataset_id':-1, 
            'dataset_pid':''
        }

    # create the pyDataverse dataset model
    ds = Dataset()
    # populate the dataset model with metadata values
    ds.title = dataset_metadata.get('title')
    ds.author = dataset_metadata.get('author')
    ds.dsDescription =  dataset_metadata.get('description')
    ds.datasetContact = dataset_metadata.get('contact')
    ds.subject = dataset_metadata.get('subject')
    ds.originOfSources = dataset_metadata.get('origin_of_sources')
    ds.license = dataset_metadata.get('license')
    ds.keyword = dataset_metadata.get('keywords')
    ds.topicClassification = dataset_metadata.get('topic_classification')
    ds.dataSources = dataset_metadata.get('data_source')
    ds.distributionDate = dataset_metadata.get('creation_date')

    # use pyDataverse to ensure that the metadata is valid
    if (ds.validate_json() == False):
        return {
            'status':False, 
            'dataset_id':-1, 
            'dataset_pid':''
        }

    # 
    # create the dataset via the dataverse api
    #

    # get the base url
    base_url = api.base_url
    # get the api token
    api_token = api.api_token
    # dataverse collection url 
    dataverse_url = dataverse_url
    # create the headers
    headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}
    # create the request url
    request_url = '{}/api/dataverses/{}/datasets'.format(base_url, dataverse_url)

    # call the requests library using the request url
    response = requests.post(request_url, headers=headers, data=ds.json())
    # get the status and message from the response
    status = int(response.status_code)

    # handle http responses
    if (not ((status >= 200) and
        (status < 300))):
        print('Error: {} - failed to create dataset {}'.format(status, dataset_metadata.get('title')))
        return {
            'status':False, 
            'dataset_id':-1, 
            'dataset_pid':''
        }
    # if success
    return {
        'status':True, 
        'dataset_id':response.json().get('data').get('id'),
        'dataset_pid':response.json().get('data').get('persistentId')     
    }

def create_datafile_metadata(inventory_df, template_csv, template_txt):
    """
    Create metadata for open metadata project datafiles based upon a template

    Parameters
    ----------
    inventory_df : DataFrame
        DataFrame containing list of datafiles to upload

    template_csv : str
        String used to generate metadata to be applied to each csv file in the inventory
        
    template_txt : str
        String used to generate metadata to be applied to each txt file in the inventory

    Return
    -------
    DataFrame
    """

    # validate parameters
    if ((inventory_df.empty == True) or
        (not template_csv) or 
        (not template_txt)):
        print('Error: One or more invalid parameters')
        return pd.DataFrame()

    # check the dataframe for required fields
    if ((not 'filename_osn' in inventory_df.columns) or
        (not 'filepath_osn' in inventory_df.columns) or
        (not 'file_type' in inventory_df.columns) or
        (not 'custom_name' in inventory_df.columns) or
        (not 'table_title' in inventory_df.columns) or
        (not 'table_type' in inventory_df.columns) or   
        #(not 'multilevel_columns' in inventory_df.columns) or
        #(not 'multilevel_rows' in inventory_df.columns) or
        #(not 'computation_ready' in inventory_df.columns) or
        (not 'series_name' in inventory_df.columns) or
        #(not 'image_handwriting' in inventory_df.columns) or
        #(not 'image_two_page' in inventory_df.columns) or
        (not 'entities' in inventory_df.columns)):
        print('Error: One or more missing required fields in inventory')
        return pd.DataFrame()

    # create an inventory
    df = pd.DataFrame()

    #
    # prepare series of values to add to metadata dataframe
    #

    # prepare file name for actual file
    all_filenames = []
    #prepare file name for custom replacement
    all_custom_names = []
    # prepare file types
    all_file_types = []
    # prepare datafile descriptions
    all_descriptions = []
    # prepare datafile tags
    all_tags = []
    # prepare mimetypes
    all_mime_types = []
    # test variable
    all_test = []

    counter = 0

    # iterate through through the inventory and create datafile metadata
    for row in inventory_df.iterrows():
        # get inventory variables
        filename = row[1].get('filename_osn')
        all_filenames.append(filename)
        custom_name = row[1].get('custom_name')
        all_custom_names.append(custom_name)
        series_name = row[1].get('series_name')
        all_test.append(row[1].get('filename_osn')) # test the series functionality
        file_type = row[1].get('file_type')
        all_file_types.append(file_type)
        file_tags = ['Data'] # tags for this particular file, init with 'Data'

        # get file entities
        entities = []
        val = row[1].get('entities')
        # ignore files with no entities
        if (not val is np.nan):
            entities = entities + str(val).split(';')
        # add entities to file tags
        file_tags = file_tags + entities

        # set file mimetype
        if (file_type == 'image'):
            all_mime_types.append('image/jpeg')
        elif (file_type == 'alto'):
            all_mime_types.append('application/xml')
        elif (file_type == 'txt'):
            all_mime_types.append('text/plain')        
        elif (file_type == 'csv'):
            all_mime_types.append('text/csv')
        else:
            all_mime_types.append('UNKNOWN')             

        # handle csv files
        if (file_type == 'csv'):
            title = row[1].get('table_title')
            if (title is np.nan):
                title = row[1].get('table_type')
            # table title for csv files included in descriptions
            desc_csv = template_csv + ' ' + title
            all_descriptions.append(desc_csv)
            file_tags.append('Table Type:{}'.format(row[1].get('table_type')))
            #file_tags.append('Multilevel Columns:{}'.format(row[1].get('multilevel_columns')))
            #file_tags.append('Multilevel Rows:{}'.format(row[1].get('multilevel_rows')))
            #file_tags.append('Computation Ready:{}'.format(row[1].get('computation_ready')))
        else:
            # set description
            desc_txt = template_txt + ' ' + series_name
            all_descriptions.append(desc_txt)

        # serialize the file tags
        all_tags.append(json.dumps(file_tags))
        counter = counter + 1

    # Add columns to the dataframe
    df['filename_osn'] = pd.Series(all_filenames)
    df['custom_name'] = pd.Series(all_custom_names)
    df['file_type'] = pd.Series(all_file_types)
    df['description'] = pd.Series(all_descriptions)
    df['mimetype'] = pd.Series(all_mime_types)
    df['tags'] = pd.Series(all_tags)
    df['test'] = pd.Series(all_test)

    return df

def python_dvuploader(api, dataverse_url, dataset_pid, data_directory, metadata_df):
    """
    Upload data files to dataverse repository using direct upload method

    Parameters
    ----------
    api : pyDataverse api
    dataverse_url : str
        Dataverse installation url (e.g., https://demo.dataverse.org)
    dataset_pid : str
        Persistent identifier for the dataset (its DOI, takes form: doi:xxxxx)
    data_directory : str
        Directory where data files are kept
    metadata_df : DataFrame
        DataFrame containing metadata about datafiles to upload

    Return
    ------
    dict
        {upload: bool, errors: list, finalize: bool}
    """

    #validate params
    if ((not api) or 
        (not dataverse_url) or
        (not dataset_pid) or
        (not data_directory) or
        (metadata_df.empty==True)):
        return False

    #error msg
    errors = []

    json_data = []
    cats = None

    #add each file in metadata_df to files list for dvuploader
    
    files = []
    
    for row in metadata_df.iterrows():
        file = row[1].get('filename_osn')
        filepath = data_directory + "/" + file
        file_name = row[1].get('custom_name')
        desc = row[1].get('description')
        mime_type = row[1].get('mimetype')

        #format tags
        tags = row[1].get('tags')
        tags_lst = eval(tags)
        
        files.append(dv.File(filepath = filepath,
                             file_name = file_name,
                             description = desc,
                             mimeType = mime_type,
                             categories = tags_lst
                            )
                    )
        
        #print('Uploading: {}/{} - {} {}'.format(data_directory, filepath, desc, mime_type))

        
    key = api.api_token
    dvuploader = dv.DVUploader(files=files)
        
    dvuploader.upload(
        api_token = key,
        dataverse_url = dataverse_url,
        persistent_id = dataset_pid,
        n_parallel_uploads= 2 #however many your installation can handle
    )

def direct_upload_datafiles(api, dataverse_url, dataset_pid, data_directory, metadata_df):
    """
    Upload Open Metadata datafiles to dataverse repository using direct upload method

    Parameters
    ----------
    api : pyDataverse api
    dataverse_url : str
        Dataverse installation url (e.g., https://demo.dataverse.org)
    dataset_pid : str
        Persistent identifier for the dataset (its DOI, takes form: doi:xxxxx)
    data_directory : str
        Directory where datafiles are kept
    metadata_df : DataFrame
        DataFrame containing metadata about datafiles to upload

    Return
    ------
    dict
        {upload: bool, errors: list, finalize: bool}
    """
    # validate paramters
    if ((not api) or
        (not dataverse_url) or
        (not dataset_pid) or
        (not data_directory) or 
        (metadata_df.empty == True)):
        return False
    
    # error messages
    errors = []

    # per file json data array
    json_data = []
    categories = None

    # upload each datafile in the metadata dataframe
    import ddu # local module
    key = api.api_token
    for row in metadata_df.iterrows():
        filename = row[1].get('filename_osn')
        custom_name = row[1].get('custom_name')
        description = row[1].get('description')
        mime_type = row[1].get('mimetype')
        categories = json.loads(row[1].get('tags'))
        print('Uploading: {}/{} - {} {}'.format(data_directory, filename, description, mime_type))

         # upload the datafile
        data = {}
        data = ddu.custom_name_direct_upload(dataverse_url, dataset_pid, key, filename, data_directory, custom_name, mime_type, retries=10)
        if (data == None):
            msg ='Warning: Failed to upload: {}'.format(filename)
            errors.append(msg)
        else:
            data['description'] = description
            data['categories'] = categories
            json_data.append(data)

    # finalize the direct upload
    status = ddu.finalize_direct_upload(dataverse_url, dataset_pid, json_data, key)

    # return errors, if any
    if (len(errors) > 0):
        return {'upload':False,'errors':errors,'finalize':status}
    else:
        return {'upload':True,'errors':[],'finalize':status}
    
def delete_datasets(api, dataverse_url):
    """
    Delete all datasets in the dataverse collection. 
    Use with caution, and only on demo.dataverse.org installation.

    Parameters
    ----------
    api : pyDataverse API
    dataverse_url : str
        Name of the dataverse collection (e.g., histd)

    Return
    ------
        bool
    """
    # get the datasets in the collection
    contents = api.get_dataverse_contents(dataverse_url, auth=True)
    # get the data
    data = contents.json().get('data')
    datasets = []
    for dataset in data:
        url = dataset.get('persistentUrl')
        pid = url.split('https://doi.org/')[1]
        datasets.append('doi:' + pid)
    # destroy the datasets
    for dataset in datasets:
        response = api.destroy_dataset(dataset, is_pid=True, auth=True)
        status = response.json()
        print('api.destroy_dataset: {}'.format(status))
    return True

def publish_datasets(api, dataverse_collection, version='major'):
    """
    Publish each dataset in a list. Logs result to log dataframe

    Parameters
    ----------
    api : pyDataverse API
    dataverse_url : str
        Name of the dataverse collection (e.g., histd)
    version: str
        Type of version update to trigger. "major" or "minor"

    Return
    ------
    dict
        {'status':bool,'message':str}
    """
    # validate parameters
    if ((not api) or
        (not dataverse_collection)):
        return {'status':False,'message':'Invalid parameter'}
    
    # get the datasets in the collection
    contents = api.get_dataverse_contents(dataverse_collection, auth=True)
    # get the data
    data = contents.json().get('data')
    datasets = []
    for dataset in data:
        url = dataset.get('persistentUrl')
        pid = url.split('https://doi.org/')[1]
        datasets.append('doi:' + pid)

    # store errors to return, keyed on pid
    errors = {}

    import requests
    # get the base url
    base_url = api.base_url
    # get the api token
    api_token = api.api_token
    # create the headers
    headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}

    # publish the datasets
    for dataset in datasets:
        # create the request url
        request_url = '{}/api/datasets/:persistentId/actions/:publish?persistentId={}&type={}'.format(base_url, dataset, version) 
        # call the requests library using the request url
        response = requests.post(request_url, headers=headers)
        
        # handle responses
        status = response.status_code
        if (not (status >= 200 and status < 300)):
            msg = 'publish_dataset::Error - failed to publish dataset: {}:{}'.format(status,dataset)
            errors[dataset] = {'status':False,'message':msg}
        else:
            msg = 'publish_dataset::Success - published dataset: {}:{}'.format(status,dataset)
            errors[dataset] = {'status':True,'message':msg}
            
    return errors

def unlock_datasets(api, dataverse_collection):
    """
    Unlock datasets that failed to publish. Usually this is due to some kind of Dataverse indexing error, not user error)

    Parameter
    ---------
    api : pyDataverse api
    dataverse_collection: str
        ID of the dataverse collection (e.g., 1924_trade_returns)

    Return
    ------
    dict
        {'status':bool,'message':str}
    """
    
    # validate parameters
    if ((not api) or
        (not dataverse_collection)):
        return {'status':False,'message':'Invalid parameter'}
    
    # get the datasets in the collection
    contents = api.get_dataverse_contents(dataverse_collection, auth=True)
    # get the data
    data = contents.json().get('data')
    datasets = []
    for dataset in data:
        url = dataset.get('persistentUrl')
        pid = url.split('https://doi.org/')[1]
        datasets.append('doi:' + pid)

    import requests
    # get the base url
    base_url = api.base_url
    # get the api token
    api_token = api.api_token
    # create the headers
    headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}
    
    #create list of locked datasets
    locked_pids = []
    
    #check dataset to see if locked & if so add to list of datasets to unlock
    for dataset in datasets: 
        # create the request url
        request_url = '{}/api/datasets/:persistentId/locks?persistentId={}'.format(base_url, dataset) 
        # call the requests library using the request url
        response = requests.get(request_url, headers=headers)
        #check status response to see if there's a lock
        ret = response.json()['data']
        if ret:
            locked_pids.append(ret[0]['dataset'])
        else:
            continue

    #store errors to return, keyed on pid
    errors = {}    
    
    #unlock list of datasets
    for pid in locked_pids:
        #create new request url
        request_url = '{}/api/datasets/:persistentId/locks?persistentId={}'.format(base_url, pid)
        #call the requests library using the request url
        response = requests.delete(request_url, headers=headers)
        #save status reponse for error return
        status = response.status_code
        #add messages depending on status response
        if (not (status >= 200 and status < 300)):
            msg = 'publish_dataset::Error - failed to unlock dataset: {}:{}'.format(status,pid)
            errors[pid] = {'status':False,'message':msg}
        else:
            msg = 'publish_dataset::Success - unlocked dataset: {}:{}'.format(status,pid)
            errors[pid] = {'status':True,'message':msg}
            
    return errors

# end document