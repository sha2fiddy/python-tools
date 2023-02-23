from json import loads
from asyncio import run

from pandas import DataFrame, concat, merge, to_datetime, json_normalize

from async_api_client import API

"""
# nest_asyncio is needed in for async code in some environments like Jupyter
# nest_asyncio requires full asyncio package, your mileage may vary
import asyncio
from nest_asyncio import apply as nest
nest()
"""

class MempoolAPI:
    """
    A client for making calls to the Mempool API using httpx.
    Uses asynchronous code which may result in rate limiting
    (or blacklisting) on the public Mempool server:
    https://mempool.space/api
    Includes a get_blocks method for fetching data for a range
    of blocks, returned in a pandas DataFrame object.
    """
    def __init__(self, url:str='https://mempool.space/api'):
        """
        Requires Mempool API server base url to initialize.
        Requires general async_api_client.py API object.
        
        param url: string, base url of Mempool API server
        """
        self.url = url
        self.api = API(self.url)
        
    
    def _compile_blocks(
        self,
        blocks:list,
        blocks_v1:list,
        cols:dict = {
            'height': 'blockheight',
            'id': 'blockhash',
            'previousblockhash': 'prev_blockhash',
            'timestamp': 'timestamp',
            'mediantime': 'blocktime_median',
            'difficulty': 'difficulty',
            'version': 'version',
            'coinbaseRaw': 'coinbase_raw',
            'merkle_root': 'merkle_root',
            'nonce': 'nonce',
            'bits': 'bits',
            'size': 'size',
            'weight': 'weight',
            'tx_count': 'tx_count',
            'reward': 'block_reward',
            'totalFees': 'fee_sum',
            'avgFee': 'fee_mean',
            'avgFeeRate': 'feerate_mean',
            'feerate_min': 'feerate_min',
            'feerate_median': 'feerate_median',
            'feerate_max': 'feerate_max',
            'feerate_pctl_10': 'feerate_pctl_10',
            'feerate_pctl_25': 'feerate_pctl_25',
            'feerate_pctl_75': 'feerate_pctl_75',
            'feerate_pctl_90': 'feerate_pctl_90',
            'pool.id': 'pool_Mempool_id',
            'pool.name': 'pool_Mempool_name',
            'pool.slug': 'pool_Mempool_slug',
            'datasource': 'datasource'
        }
    ) -> DataFrame:
        """
        Hidden method for parsing and compiling a list of blocks
        fetched asynchronously from the Mempool API.
        Meant only to be used within the fetch_blocks method.
        Requires blocks and blocks_v1 list objects returned from
        the /block/ and /v1/block/ endpoints.
        Requires a cols dictionary object for reordering and renaming attributes.
        
        param blocks: list, list of block JSON strings from /blocks/ endpoint
        param blocks_v1: list, list of block_v1 JSON strings from /v1/blocks/ endpoint
        param cols: dict, dictionary used to reorder and rename attributes.
        """
        try:
            if not len(blocks) == len(blocks_v1):
                print('Error compiling blocks, attribute lists of differenth lengths')
                print(f'blocks length: {len(blocks)}, blocks_v1 length: {len(block_v1)}')
                return None
            else:
                # Merge blocks objects; mediantime is the only attribute missing from blocks_v1
                blocks, blocks_v1 = [loads(b) for b in blocks], [loads(b) for b in blocks_v1]
                df, df_mediantime = DataFrame(blocks_v1), DataFrame(blocks)[['id', 'mediantime']]
                df = merge(df, df_mediantime, on='id')
                del blocks, blocks_v1, df_mediantime
                
                # Parse out nested objects
                df_extras = json_normalize(df['extras'])
                feerate_cols = [f'feerate_{p}' for p in [
                    'min', 'pctl_10', 'pctl_25', 'median', 'pctl_75', 'pctl_90', 'max']]
                df_feerates = DataFrame(df_extras['feeRange'].to_list(), columns=feerate_cols)
                df = concat([df, df_extras, df_feerates], axis=1)
                del df_extras, df_feerates
                
                # Convert datetime, add datasource, reindex by blockheight
                df['timestamp'] = to_datetime(df['timestamp'], unit='s')
                df['datasource'] = self.url
                df.sort_values('height', ignore_index=True, inplace=True)
                
                # Drop, reorder, rename columns
                df = df[list(cols)]
                df.rename(columns=cols, inplace=True)
                print(f'Dataframe shape: {df.shape}')
                return df
        
        except:
            raise Exception('Error compiling blocks')

    
    async def get_blocks(self, blockheight_start:int, blockheight_end:int) -> DataFrame:
        """
        Method for fetching a set of block attributes for
        a range of blocks from the Mempool API, in a pandas DataFrame.
        Requires blockheight_start and blockheight_end integers,
        indicating the range of blocks to fetch.
        
        param blockheight_start: int, first in sequence of blockheights to fetch
        param blockheight_end: int, last in sequence of blockheights to fetch
        """
        try:
            current_blockheight = int(self.api.call('/blocks/tip/height'))
            if not 0 <= blockheight_start <= blockheight_end <= current_blockheight:
                print('Invalid blockheight! Must be in range:' + '\n' +\
                      f'0 <= blockheight_start <= blockheight_end <= {current_blockheight}')
                return None

            blockheights = range(blockheight_start, blockheight_end + 1)
            blockhashes = run(self.api.async_calls([f'/block-height/{b}' for b in blockheights]))
            # There are two versions of the block endpoint, neither has complete list of attributes
            blocks = run(self.api.async_calls([f'/block/{b}' for b in blockhashes]))
            blocks_v1 = run(self.api.async_calls([f'/v1/block/{b}' for b in blockhashes]))
            df = self._compile_blocks(blocks, blocks_v1)
            return df

        except:
            raise Exception('Error fetching block data')


"""
# Example usage:
from asyncio import run


url = 'https://mempool.space/api'
api = MempoolAPI(url)

df = run(api.get_blocks(750000, 750100))
"""
