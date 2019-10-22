import pandas as pd
import numpy as np
import uuid
from sqlalchemy.types import String, TIMESTAMP, Boolean
from sqlalchemy.dialects.postgresql import UUID

import configparser

config = configparser.ConfigParser()
config.read('./config.ini')
db_config = config['db_config']

user = db_config['user']
pwd = db_config['pwd']
db = db_config['db']
host = db_config['host']
fname = db_config['fname']

engine = 'postgresql+psycopg2://{user}:{pwd}@{host}/{db}'.format(user=user, pwd=pwd, host=host, db=db )
now = pd.datetime.now()


def read_brand_csv(fname):
    col_map  ={'brand_name':'entity_name',
          'maps':'location'}

    df = pd.read_csv(fname, header=0)
    df.rename(columns=col_map, inplace=True)
    df['id'] = [uuid.uuid4() for _ in range(df.shape[0])]
    return df

def read_db(engine):
    df_entities = pd.read_sql('select * from entities', engine)
    df_tags = pd.read_sql('select * from sustainability_metric_values', engine)
    return df_entities, df_tags


def create_business(dataframe, col):
    df = dataframe.copy(deep=True)
    df['inserted_at'] = now
    df['updated_at'] = now
    #assigning uuid
    df['import_id'] = uuid.uuid4()
    df['entity_type'] = 'Business'
    return df[col]

def filter_new_brands(df, df_entities):
    df_sub = df[~df['entity_name'].isin(df_entities['entity_name'])]
    return df_sub

def create_business_attributes(df, col):
    df_att = df.loc[:, col]
    df_att.rename(columns = {'id':'business_entity_id'}, inplace=True)
    df_att['id'] = [uuid.uuid4() for _ in range(df_att.shape[0])]
    df_att['is_producer'] = 1
    df_att['is_distributor'] = 1
    df_att['inserted_at'] = now
    df_att['updated_at'] = now
    df_att['show_price'] = 1
    df_att['brochure'] = np.nan
    df_att['is_claimed'] = 0
#     df_att.set_index('id', inplace=True)
    return df_att

def create_business_tags(df, df_tags):
    #Todo: Tags that are currently not in the db are ignored
    tag_col = [x for x in df.columns if x.startswith('tag')]
    df_tag_sub = df[tag_col + ['id']]
    df_business_tag = pd.DataFrame(columns=['id', 'business_entity_id', 'sustainability_metric_value_id'])

    for k,v in df_tag_sub.set_index('id').iterrows():
        # removing null tags
        tag_list = v.dropna().tolist()
        for t in tag_list:
            tag_id=0
            # checking if the tag is present in sustainability metric_values
            if t in df_tags['name'].values:
                tag_id = df_tags[df_tags['name']==t]['id'].values[0]
                row_dict = {'id': uuid.uuid4(),
                            'business_entity_id':k,
                            'sustainability_metric_value_id':tag_id
                }
                df_business_tag = df_business_tag.append(row_dict, ignore_index=True)
            print(k, tag_id, t)
    return df_business_tag

def insert_to_business_db(df, engine):

    df.to_sql('entities', engine, if_exists='append',index= False, dtype={
        'id':UUID(as_uuid=True),
        'entity_name': String, 'mobile':String, 'email': String, 'location': String,
        'address':String, 'entity_type': String, 'city':String,
        'inserted_at': TIMESTAMP, 'updated_at': TIMESTAMP,
        'import_id':UUID(as_uuid=True), 'image': String, 'area': String, 'state': String
    } )

    print("Database insert successfull")

def insert_to_business_attributes_db(df, engine):

    df.to_sql('business_attributes',engine, if_exists = 'append', index=False, dtype={
        'id': UUID(as_uuid=True),
        'is_producer': Boolean, 'is_distributor': Boolean,
        'business_entity_id': UUID(as_uuid=True),
        'inserted_at': TIMESTAMP, 'updated_at': TIMESTAMP,
        'show_price': Boolean, 'description': String,
        'cover_image': String, 'website': String,
        'brochure': String, 'is_claimed': Boolean,
        'show_in_av':Boolean, 'show_outside': Boolean, # Todo: Change the column name
        'bio': String # This needs to get added
    } )
    print("Data inserted into business_metrics")


def insert_to_business_metrics_db(df, engine):

    return

if __name__ == "__main__":

    # reading necessary data
    df = read_brand_csv(fname)
    df_entities, df_tags = read_db(engine)

    # filtering for brands that are not already present
    df = filter_new_brands(df,df_entities)

    entity_columns = df_entities.columns.tolist() #bio isn't included at the moment
    att_col = ['id', 'show_leap', 'show_consciously', 'website', 'bio', 'description', 'cover_image']

    df_business = create_business(df,entity_columns)
    df_business_att = create_business_attributes(df,att_col )
    df_business_tags = create_business_tags(df,df_tags )

    df_business.to_csv('./business.csv')
    df_business_att.to_csv('./business_att.csv')
    df_business_tags.to_csv('./business_tags.csv')

    insert_to_business_db(df_business, engine)

