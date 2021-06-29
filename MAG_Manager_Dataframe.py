#%% ================================================================
"""
Imports
"""

# import util tools
import MAG_Util as MAG_Util


#%% ================================================================
"""
Dataframe manager class
Manages the dataframes, allowing us to easily load dataframes for resuability
"""

class Manager_Dataframe:

    def __init__(self, argv_filename=None):
        """
        The init function store the name of the files to be loaded into dataframes
            argv_filename is the filename of CSV data to be read into dataframes
                if single filename (string) then it is a file that contain all filenames
                if multiple filenames (dictionary) then it is already a dictionary of filenames
        """
        # the attributes
        self._filenames = {}
        self._dataframes = {}
        # check filenames are provided or not
        if argv_filename is None:
            pass
        # if single filename, it is the file that contains all of the filenames
        elif isinstance(argv_filename, str):
            self._filenames = MAG_Util.load_file(
                argv_filename=argv_filename,
                argv_type="dictionary",
                argv_separator=","
                )
        # if it is a dictionary of filenames
        elif isinstance(argv_filename, dict):
            for current_key in argv_filename:
                self._filenames[current_key] = argv_filename[current_key]

    def load_dataframes(self, argv_dropNA=False):
        """
        Load dataframe from the filenames, into as an attribute of the manager class
            argv_dropNA is the flag to determine if missing values would be dropped wjhen the dataframe is read
        """
        # check if there is data to load
        if len(self._filenames) == 0:
            return False
        # load in the data
        self._dataframes = {}
        for current_fileID in self._filenames:
            current_filename = self._filenames[current_fileID]
            current_dataframe = MAG_Util.load_into_dataframe(
                argv_filename=current_filename,
                argv_dropNA=argv_dropNA
                )
            self._dataframes[current_fileID] = current_dataframe

    def get_dataframe(self, argv_id=None):
        """
        Return the dataframe of choice
            argv_id is the dataframe ID to retrieve
                if left as None, retrieve all of the dataframes (dictionary)
        """
        if argv_id is None:
            return self._dataframes
        if argv_id in self._dataframes:
            return self._dataframes[argv_id]
        return None

    def print_filenames(self, argv_id=None):
        """
        Print the filenames
            argv_id is the dataframe ID to retrieve the filename which generate the dataframe
                if left as None, print out all of the dataframe filenames
        """
        if argv_id is None:
            for current_id in self._filenames:
                current_filename = self._filenames[current_id]
                print(str(current_id) + ": " + str(current_filename))
        elif argv_id in self._filenames[argv_id]:
            current_filename = self._filenames[argv_id]
            print(str(argv_id) + ": " + str(current_filename))

    def print_dataframes(self, argv_id=None):
        """
        Print the dataframe
            argv_id is the dataframe ID to print out the details (summary information)
                if left as None, would print out all of the dataframes information
        """
        if argv_id is None:
            for current_id in self._dataframes:
                print(current_id)
                current_dataframe = self._dataframes[current_id]
                print(current_dataframe.info())
                print(current_dataframe.head())
        elif argv_id in self._dataframes:
            print(argv_id)
            current_dataframe = self._dataframes[argv_id]
            print(current_dataframe.info())
            print(current_dataframe.head())

    def split_dataframe(self, argv_id=None, argv_col="", argv_inplace=False):
        """
        Split the single dataframe into multiple dataframes based on unique values in the column
            argv_id is the dataframe ID to be splitted
                if left as None, the last dataframe would be splitted (just being lazy for single dataframes)
            argv_col is the column name where the datagrame would be splitted accordingly
                split using the helper function split_dataframes_byCol()
                will split based on unique value in the column
            argv_inplace is the boolean value towards replacing all of the dataframes with the splitted one
                default value is False
        Returns True if process is successul
        """
        # get the dataframe based on the ID
        current_dataframe = None
        if argv_id is None:
            for current_id in self._dataframes:
                current_dataframe = self._dataframes[current_id]
        elif argv_id in self._dataframes:
            current_dataframe = self._dataframes[argv_id]
        else:
            return False
        # split the dataframe       
        # backup incase cudf got bug 
        # current_dfs = MAG_Util.split_dataframes_byCol(argv_df=current_dataframe.to_pandas(), argv_col=argv_col)
        current_dfs = MAG_Util.split_dataframes_byCol(argv_df=current_dataframe, argv_col=argv_col)
        # check if it is in-place
        if argv_inplace:
            self._dataframes = current_dfs
        else:
            for current_id in current_dfs:
                self._dataframes[current_id] = current_dfs[current_id]
        # done
        return True