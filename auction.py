import sys, os

class Auction:
    """
       This class is responsible of storing and handling information
       related to the auction requested by the GUI system.

    Attributes:
       CodeId (str): String identifying a single auction reference.
                     Any string with less of 15 characters is valid.
                     However, better if white spaces are avoided.

    .. _Google Python Style Guide:
       https://google.github.io/styleguide/pyguide.html
    """


    def __init__(self,CodeId:str):
        """ 
        Constructor function for the Auction class

        :param str CodeId: Auction Identification String.

        :returns: None

        """
        self._launched = 0
        self._started  = 0
        self._ended    = 0
        self.code = CodeId
        self.matype = 'Orders'
        self._plants= []
        self._all_plants = []
        self._all_objs   = []
        self._smats  = []
        self._lmats  = self._omats = []

    def get_codeid(self):
        """ 
        Function returning the CodeId for the auction.

        :returns: Auction Identification String.

        :rtype: str
        """

        return(self.code)

    def set_materials(self,matype:str,plants:list):
        """ 
        Function establishing the Auction parameters of
        materials and resources.

        :param str matype: Either Coils or Orders
        :param list plants: List of resources involved in the Auction
        """
        self.matype = matype
        self._plants = plants

    def get_materials(self):
        """ 
        Function returning the Auction parameters of materials and resources

        :returns: Struct of matype (str): Either Coils or Orders, 
            plants (list): List of resources involved in the Auction

        :rtype: struct
        """
        return({self.matype,self._plants})

    def set_params(self, htmscr):
        """
        Function parsing the html into UI attributes.

        :param obj htmscr: The html object with the settings selected by the user.
        """
        self.matype = htmscr.__getitem__('ag-matypes').value
        self._plants= htmscr.__getitem__('ag-resources').value
        if len(htmscr.__getitem__('ag-resources').options) > 0:
            self._all_plants = [j['value'] for j in htmscr.__getitem__('ag-resources').options]
        if len(htmscr.__getitem__('ag-materials').options) > 0:
            self._all_objs = [j['value'] for j in htmscr.__getitem__('ag-materials').options]
        if len(htmscr.__getitem__('ag-materials').value)>0:
            self._smats   = htmscr.__getitem__('ag-materials').value
        vtxt         = htmscr.__getitem__('ag-ref')
        self._lmats  = self._omats = []
        if self.matype == 'Coils':
            self._lmats  = list(set(self._all_objs) - set(self._smats))
        else:
            self._omats  = list(set(self._all_objs) - set(self._smats))
        if 'value' in vtxt:
            self.code= vtxt.value


    def get_params(self):
        """
        Function recovering auction's parameters.

        :returns: tuple(matype,plnts,all_plnts,objs,all_objs,reftxt)
           WHERE
           str     matype    is the type of object being processed Coils/Orders.
           list    plnts     is the list of selected plants.
           list    all_plnts is the list of all plants.
           list    objs      is the list of selected materials to be discarded.
           list    allobjs   is the list of all materials.
           str     reftxt    is the auction codeId.
        """
        res = (self.matype,self._plants,self._all_plants,self._smats, \
                self._all_objs,self.code)
        return(res)


    def set_codeid(self,code: str):
        """ 
        Function to setup the code when it requires update

        :param str CodeId: Auction Identification String.
        """
        self.code = code

    def set_lists(self,rftxt:str, lmats:list,mtyp:str, smats:list):
        """ 
        Function setting up the list of materials and plants relevant
        for the auction.

        :param list lmats: List of materials interesting to the Auction
        :param list omats: List of materials' orders interesting to the Auction
        :param list smats: List of resources interesting to the Auction
        """
        self.code = rftxt
        self.matype = mtyp
        if self.matype == 'Coils':
            self._lmats = lmats
            self._omats = []
        else:
            self._omats = lmats
            self._lmats = []
        self._smats = smats

    def get_lmats(self):
        """ 
        Function returning the list of materials of interest for this Auction

        :returns: List of materials interesting the Auction.

        :rtype: list
        """
        return(self._lmats)

    def get_omats(self):
        """ 
        Function returning the list of materials' orders of interest 
        for this Auction.

        :returns: List of materials' orders interesting the Auction.

        :rtype: list
        """
        return(self._omats)

    def get_smats(self):
        """ 
        Function returning the list of resources of interest for this Auction

        :returns: List of resources interesting the Auction.

        :rtype: list
        """
        return(self._smats)

    def set_status(self,start):
        """ 
        Function establising the current status for the auction

        :param str start: Auction status according to the following code
                         'L' => Launched, 'G' => Started, 'E' => Ended.
        """
        if start == 'L':
           self._launched = 1
        if start == 'G':
           self._started  = 1
        if start == 'E':
           self._ended    = 1

    def get_status(self):
        """ 
        Function returning the status according to a scale
        distinguishing when the auction is not launched, launched,
        started or ended.

        :returns: Status on base 10.

        :rtype: int
        """
        return(100*self._ended+10*self._started+self._launched)

    def set_message(self,msg):
        """ 
        Function establishing the message describing the current status

        :param str msg: Status message.
        """
        self._msg = msg

    def get_message(self):
        """ 
        Function recovering the message describing the current status

        :returns: message to be returned.

        :rtype: str
        """
        return(self._msg)
