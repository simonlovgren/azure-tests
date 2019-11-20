from azure.cosmos import exceptions, CosmosClient, PartitionKey
import json, sys, uuid

# ----------------------------------------------------------------------------------------------------------------------
# Globals
# ----------------------------------------------------------------------------------------------------------------------

endpoint = 'https://aztestsite.documents.azure.com:443/'
key      = ''

# ----------------------------------------------------------------------------------------------------------------------
# Functions
# ----------------------------------------------------------------------------------------------------------------------

def loadKeys():
    '''
    Load keys from keys.json
    '''
    global key
    with open( 'keys.json', 'r' ) as keyFile:
        keysRaw = json.load( keyFile )
    if ( 'primary' in keysRaw.keys() ):
        key = keysRaw[ 'primary' ]
        return True
    else:
        print( f'Unable to load primary key', file = sys.stderr )
    return False

def getItemObject( existingItem = None, category = None, description = None, name = None, isComplete = False ):
    '''
    Creates an item object. Uses values from existingItem if supplied.
    Optional values may be added via arguments OR afterwards.
    '''
    if ( existingItem is not None ):
        item = {
            "id":           existingItem["id"],
            "category":     existingItem["category"],
            "name":         existingItem["name"],
            "description":  existingItem["description"],
            "isComplete":   existingItem["isComplete"]
        }
    else:
        item = {
            "id":           str( uuid.uuid4() ),
            "category":     category if ( category is not None ) else "",
            "name":         name if ( name is not None ) else "",
            "description":  description if ( description is not None ) else "",
            "isComplete":   isComplete
        }
    return item

# ----------------------------------------------------------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------------------------------------------------------

def main( args = None ):
    if ( False == loadKeys() ):
        return -1

    # Create cosmos client
    client = CosmosClient( endpoint, key )

    # Connect to database
    database = client.get_database_client( "Tasks" )

    # Get container
    container = database.get_container_client( "Items" )

    # List items in database
    query = "SELECT * FROM c"
    items = list( container.query_items(
        query                        = query,
        enable_cross_partition_query = True
    ) )
    for item in items:
        print( f'{item["category"]} / {item["name"]}  / {item["description"]} / {item["isComplete"]}' )

    # Query for single item & update it
    query = "SELECT * FROM c WHERE c.name = 'groceries'"
    items = list( container.query_items(
        query                        = query,
        enable_cross_partition_query = True
    ))

    if ( len( items ) == 1 ):
        # Update isComplete to "not current value"
        item = items[0]
        print( item )
        updatedItem = getItemObject( existingItem = item )
        updatedItem[ "isComplete" ] = ( not item[ "isComplete" ] )
        print( updatedItem )
        container.replace_item(
            item = item,
            body = updatedItem,
        )
    elif ( len( items ) > 1 ):
        print( f'Multiple entries matching query...' )
    else:
        print( f'No entry matching query [{query}] in database.' )

    # Insert item into database
    newItem = getItemObject(
        name        = "new testitem",
        description = "Test of inserting an item (document) to the database.",
        category    = "test"
    )
    container.create_item( newItem )

    # List items in database
    query = "SELECT * FROM c"
    items = list( container.query_items(
        query                        = query,
        enable_cross_partition_query = True
    ) )
    for item in items:
        print( f'{item["category"]} / {item["name"]}  / {item["description"]} / {item["isComplete"]}' )


    # Request charge for database interaction
    request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
    print('Query returned {0} items. Operation consumed {1} request units'.format(len(items), request_charge))

    return 0

if __name__ == "__main__":
    # parse args
    sys.exit( main() )