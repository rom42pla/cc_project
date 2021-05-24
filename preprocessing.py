import pandas as pd
import sys 

file = sys.argv[1]

def assign_rating_from_views(df):
    
    ''' 1. Importing data and defining the rating according to the number of views of a specifict product by a specific user.
    2. Selecting purchase event only and assigning the corresponding rating'''
    
    purchases_users = df[df.event_type == 'purchase'].user_id.tolist()
    rating_dict = df[df.user_id.isin(purchases_users)][df.event_type == 'view'].groupby(['user_id','product_id'])['event_type'].count().to_frame()['event_type'].to_dict()
    df = df[df.event_type == 'purchase']
    df['rating'] = [0]*df.shape[0]
    
    for i,row in df.iterrows():
        p = row.product_id
        u = row.user_id
        try:
            df['rating'][i] = rating_dict[tuple([u,p])]
        except:
            pass

    return df.drop(['event_type'],axis=1)
    

def remapping(df,column):
    
    ''' ID small integers remapping '''
    remapping = dict(zip(df[column].unique(),range(1,len(df[column].unique())+1)))
    return df.replace({column: remapping})



def __main__():
    
    df = pd.read_csv(file, sep =',',usecols=['event_time','event_type','product_id','category_id','user_id'])
    df = assign_rating_from_views(df)
    df = remapping(df,'product_id')
    df = remapping(df,'category_id')
    df = remapping(df,'user_id')
    df.to_csv(file,index=False)
    
   
__main__()
