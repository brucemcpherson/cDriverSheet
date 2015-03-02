
/** wrapper
 */
function createDriver (handler,siloId,driverSpecific,driverOb, accessToken) {
    return new DriverMemory(handler,siloId,driverSpecific,driverOb, accessToken);
}
/**
 * DriverMemory
 * @param {cDataHandler} handler the datahandler thats calling me
 * @param {string} tableName this is filename
 * @return {DriverMemory} self
 */
function getLibraryInfo () {
  return {
    info: {
      name:'cDriverMemory',
      version:'2.2.0',
      key:'M96uVZRXXG_RY3c2at9V6tSz3TLx7pV4j',
      share:'https://script.google.com/d/101pVFakzEfvHquUFOmZafAzfBAGSotgH56IqVcGmWNBu7J0sweklqyCB/edit?usp=sharing',
      description:'in memory dbabstraction driver'
    },
    dependencies:[
      cUseful.getLibraryInfo()
    ]
  }; 
}

// this can be used for in memory querying of a data set
// a JSON object can be manipulated to provide database like characteristics
// data is stored as [ {key:someuniquekey, data:{},...]
 
var DriverMemory = function (handler,tableName,driverSpecific) {
  var siloId = tableName;
  var self = this;
  var dbId = driverSpecific;
  var parentHandler = handler;
  var enums = parentHandler.getEnums();  
  var handle, handleError, handleCode , handleIds, handleKey; 
  var content_ = [];
  var transactionBox_ = null;

  
  // im able to do transactions
  self.transactionCapable = true;
  
  // i dont need any locking
  self.lockingBypass = true;
  
  // i am aware of transactions and know about the locking i should do
  self.transactionAware = true;
  
  
 /**
  * begins transaction and store current content
  * @param {string} id transaction id
  */ 
  self.beginTransaction = function (id) {
    transactionBox_ = {
      id: id,
      content: null
    };
  };
  
  self.getTransactionBox = function () {
    return transactionBox_;
  };
  
  self.transactionData = function () {
    self.getTransactionBox().content = parentHandler.clone(content_);
    return parentHandler.makeResults (enums.CODE.OK);
  };
  
 /**
  * commits transaction
  * @param {string} id transaction id
  * @return {object} a normal result package
  */ 
  self.commitTransaction = function (id) {
    
    // double check we are committing the correct transaction
    if (!self.isTransaction (id)) {
      return parentHandler.makeResults (enums.CODE.TRANSACTION_ID_MISMATCH);
    }
    
    // with memory there is nothing more to do- memory is already committed
    transactionBox_= null;
    return parentHandler.makeResults(enums.CODE.OK);
  };
  
 /**
  * roll back transaction - resets memory to beginnging of transaction
  * @param {string} id transaction id
  * @return {object} a normal result package
  */ 
  self.rollbackTransaction = function (id) {
    
    // double check we are committing the correct transaction
    if (!self.isTransaction (id)) {
      return parentHandler.makeResults (enums.CODE.TRANSACTION_ID_MISMATCH);
    }
    
    // roll back content
    self.makeContent(transactionBox_.content);
    transactionBox_= null;
    return parentHandler.makeResults(enums.CODE.OK);
  };
  
 /**
  * checks that the transaction matches the one stored
  * @param {string} id transaction id
  * @return {boolean} whether id matches
  */ 
  self.isTransaction = function (id) {
    return transactionBox_ && transactionBox_.id === id ;
  };
  
  self.getType = function () {
    return enums.DB.MEMORY;
  };
  
  self.getDbId = function () {
    return siloId;
  };
  
  self.getParentHandler = function () {
      return parentHandler;
  };

  handle = self;
  
  /** return self 
   * @return {DriverMemory} the self
   */
  self.getDriveHandle =  function () {
    return handle;
  };
  
  
  /**
   * DriverMemory.getTableName()
   * @return {string} table name or silo
   */
  self.getTableName = function () {
    return siloId;
  };
  
  /**
   * DriverMemory.getContent_() returns a copy of the content
   * @return {object}
   */
  self.getContent_ = function () {
    return  parentHandler.clone(content_) ;
  };
  
  /**
   * DriverMemory.makeContent() sets the content
   * @param {object} the content
   * @return {DriverMemory} self
   */
  self.makeContent = function (data) {
    content_ = data  ;
    return self;
  };
  
  /**
   * DriverMemory.takeContent() gets the content
   * @return {object} the  content
   */
  self.takeContent = function () {
    return  content_   ;
  };
  
  /**
   * DriverMemory.setContent_() takes a copy of data to be current content
   * @return {void}
   */
  self.setContent_ = function (data) {
     content_ = parentHandler.clone(data);
  };
  return self;
  
};


/** create the driver version
 * @return {string} the driver version
 **/ 
DriverMemory.prototype.getVersion = function () {
  return getLibraryInfo().info.name+':'+getLibraryInfo().info.version;
};

/** each saved records needs a unique key in orchestrate
* @return {string} a unique key
*/
DriverMemory.prototype.generateKey = function () {
  var self = this;
  return self.getParentHandler().generateUniqueString();
};

/**
 * DriverMemory.query()
 * @param {object} queryOb some query object 
 * @param {object} queryParams additional query parameters (if available)
 * @param {boolean} keepIds whether or not to keep driver specifc ids in the results
 * @return {object} results from selected handler
 **/
DriverMemory.prototype.query = function (queryOb,queryParams,keepIds) {
  var self = this;
  var enums = self.getParentHandler().getEnums();  
  var result =null,driverIds=[],handleKeys=[];
  var handleCode = enums.CODE.OK, handleError='';
  
  try {
    
    result =  self.getContent_();

    // apply anyfilters
    var pr = self.getParentHandler().processFilters (queryOb, result); 
    
    handleCode =pr.handleCode;
    handleError = pr.handleError;
    if (handleCode === enums.CODE.OK) {
      result = pr.data;
    }
    
    // LIMIT & SORT & skip
    if (handleCode===enums.CODE.OK) {
      var pr = self.getParentHandler().processParams( queryParams,result);
      handleCode =pr.handleCode;
      handleError = pr.handleError;
      
      // get rid of ids if necessary
      if (handleCode === enums.CODE.OK) {
        result = pr.data.map(function(d) {
          if (keepIds) {
            driverIds.push(d.keys);
            handleKeys.push(d.keys);
          }
          return d.data;
        });
      }
      
    }
  }
  catch(err) {
    handleError = err;
    handleCode = enums.CODE.DRIVER;
  }
  
  
  return self.getParentHandler().makeResults (handleCode,handleError,result,keepIds ? driverIds :null,keepIds ? handleKeys:null);
};

/**
 * Driver.update()
 * @param {string} key the unique return in handleKeys for this object
 * @param {object} ob what to update it to
 * @param {string} optPlant where to pant the key in the data if required
 * @return {object} results from selected handler
 **/
DriverMemory.prototype.update = function (keys,obs,optPlant) {
  var self = this;
  var result =null;
  var enums = self.getParentHandler().getEnums();  
  var keyName = 'key';
  var handleError='', handleCode=enums.CODE.OK
  
  if (!Array.isArray (obs)) obs = [obs];
  if (!Array.isArray (keys)) keys = [keys];
  
  if(keys.length !== obs.length && obs.length !== 1) {
    return  self.getParentHandler().makeResults (enums.CODE.KEYS_AND_OBJECTS,'objects- ' + obs.length + ' keys- ' + keys.length,result);
  }
  
  try {
    // the data
    var content = self.getContent_(); 

    // update each row matching a key
    keys.forEach (function(d,i) {
      
      // various strategies for matching the key
      var idx = find_(d,content,keyName);
      
      // oops no match
      if (idx === -1) {
        handleCode = enums.CODE.NOMATCH;
      }
      
      // update with new data and carry forward the key in the data if needed
      else {
        content[idx].data =  obs.length === 1 ? obs[0] : obs[i];
        if (optPlant) content[idx].data[optPlant] = d;
      }
    });
    
    self.setContent_(content);
  }
  catch(err) {
    handleError = err;
    handleCode =  enums.CODE.DRIVER;
  }

  function find_ (id,content, kName) {
    
    var idx = -1;
    content.some(function(d,i) {
      if (compareKeys_ ( d ,id ,kName )) idx = i;
      return idx === i;
    });
    return idx;

  }
  
  return  self.getParentHandler().makeResults (handleCode,handleError);
};


  
/**
 * Driver.get()
 * @param {string} key the unique return in handleKeys for this object
 * @param {boolean} keepIds whether or not to keep driver specifc ids in the results
 * @param {string} optKeyName key name to match on (default 'key')
 * @return {object} results from selected handler
 **/
DriverMemory.prototype.get = function (keys,keepIds,optKeyName) {
  var self = this;
  var result =null;
  var enums = self.getParentHandler().getEnums();  
  var result =null;
  var handleError='', handleCode=enums.CODE.OK;
  var driverIds = [], handleKeys = [];
  var keyName = optKeyName || 'key';
  // only returns one
  if (!Array.isArray(keys)) keys = [keys];
  // the data
  
  result = self.getContent_().filter (function(d) {
    return keys.some( function(e) {
      return compareKeys_ (d , e ,keyName );
    });
  });
  if (!result.length) {
    handleCode = enums.CODE.NOMATCH;
  }
  else {
    // get rid of ids if necessary
    result = result.map(function(d) {
      if (keepIds) {
        driverIds.push(d.keys);
        handleKeys.push(d.keys);
      }
      return d.data;
    });
  }

  return self.getParentHandler().makeResults (handleCode,handleError,result,keepIds ? driverIds :null,keepIds ? handleKeys:null);
};

/**
 * DriverMemory.count()
 * @param {object} queryOb some query object 
 * @param {object} queryParams additional query parameters (if available)
 * @return {object} results from selected handler
 **/ 
DriverMemory.prototype.count = function (queryOb,queryParams) {
  var self = this;
  var enums= self.getParentHandler().getEnums();
  var result =[];
  var handleCode = enums.CODE.OK, handleError='';
  
  try {
    // start with a query
    var queryResults = self.query (queryOb, queryParams );
    result = [{count:queryResults.data.length}];
  }
  catch(err) {
    handleError = err + "(counting "+ self.getTableName()+" in memory)";
    handleCode =  enums.CODE.DRIVER;
  }
  
  return self.getParentHandler().makeResults (handleCode,handleError,result);
  
};
  
/**
* DriverMemory.save()
* @param {Array.object} obs array of objects to write
* @return {object} results from selected handler
*/
DriverMemory.prototype.save = function (obs,optKey) {
  var self = this;
  var enums= self.getParentHandler().getEnums();
  var handleError='', handleCode=enums.CODE.OK;
  var toAdd;
 
  try {
    var oldContent = self.getContent_();
    toAdd = obs.map (function(d,i) {
      return self.makeMemoryEntry (d,i+oldContent.length,optKey);
    });

    self.setContent_(oldContent.concat(toAdd));
    var newContent = self.getContent_();
    if (newContent.length != obs.length + oldContent.length) {
      handleCode = enums.CODE.DRIVER_ASSERTION;
      handleError = 'after saving, length was ' + newContent.length + ' but should have been ' + oldContent.length + queryResults.handleKeys.length;
    }
    // so now we need to patch the row numbers  
  }
  catch(err) {
    handleError = cUseful.showError(err) + "(writing "+ self.getTableName()+" to memory)";
    handleCode =  enums.CODE.DRIVER;
  }
  var tr = self.getParentHandler().makeResults (handleCode,handleError,obs,undefined,self.getKeys(toAdd))
  return tr;
}; 

DriverMemory.prototype.getKeys = function (entries) {
 
   return (entries || []).map (function(d) {
     return d.keys ? {keys:d.keys} : {key:d.key};
   });
   
}; 

DriverMemory.prototype.makeMemoryEntry = function (item, index, optKey) {
  var self  = this;
  
  // this is for preserving keys between sessions
  var key;
  
  // this is transitional .. eventually I'll harmonize the key structure between delegated drivers since clean up at version 2.2
  if (optKey) {
    if (item.hasOwnProperty(optKey)) {
      key = item[optKey];
    }
    else {
      if (item.hasOwnProperty(keys)) {
        key = item.keys.key;
      }
      else {
        item = key.key;
      }
    }
  }
  else {
    key = self.generateKey();
  }
  
  if (!key) throw 'programming error generating key';
  
  var d = { 
    data: item,
    keys: {
      key: key,
      row: index+1
    }
  };
  return d;
}

function compareKeys_ (item, key,keyName) {
  return (cUseful.isObject(key) ? key[keyName] : key) === item.keys[keyName]  ;
}

/**
 * DriverMemory.remove()
 * @param {object} queryOb some query object 
 * @param {object} queryParams additional query parameters (if available)
 * @param {object} optKeyName optional keyname to match on
 * @return {object} results from selected handler
 **/  
DriverMemory.prototype.remove = function (queryOb,queryParams,optKeyName) {
  var self = this;
  var enums= self.getParentHandler().getEnums();
  var result =null;
  var handleError='', handleCode=enums.CODE.OK;
  var keyName = optKeyName || 'key';
  
  try {
    // start with a query
    
    var queryResults = self.query (queryOb, queryParams , true);


    if (handleCode === enums.CODE.OK && queryResults.data.length > 0 ) {
      var oldContent = self.getContent_();
      self.setContent_(oldContent.filter( function(d) {
        return !queryResults.handleKeys.some( function (k) {
          return compareKeys_ (d , k ,keyName ) ;
        });
      }));
      var newContent = self.getContent_();
      if (oldContent.length != queryResults.handleKeys.length + newContent.length) {
        handleCode = enums.CODE.DRIVER_ASSERTION;
        handleError = 'after removing, length was ' + newContent.length + ' but should have been ' + oldContent.length - queryResults.handleKeys.length;
      }
    }
  }
  catch(err) {
    handleError = cUseful.showError(err) + "(writing "+ self.getTableName()+" to memory)";
    handleCode =  enums.CODE.DRIVER;
  }
  
  return self.getParentHandler().makeResults (handleCode,handleError);
};

/**
 * DriverMemory.removeByIds()
 * @param {Array.string} keys array of keys to match
 * @param {Array.string} optKeyName optional key name to target
 * @return {object} results from selected handler
 */  
DriverMemory.prototype.removeByIds = function (keys,optKeyName) {
  var self = this;
  var enums= self.getParentHandler().getEnums();
  var result =null;
  var handleError='', handleCode=enums.CODE.OK;
  var keyName = optKeyName || 'key';
  
  try {
    // get the current content
    var oldContent = self.getContent_();
    self.setContent_(oldContent.filter( function(d) {
      return !keys.some( function (k) {
        return compareKeys_ (d , k ,keyName ) ;
      });
    }));
    var newContent = self.getContent_();
    if (oldContent.length != keys.length + newContent.length) {
      handleCode = enums.CODE.DRIVER_ASSERTION;
      handleError = 'after removing, length was ' + newContent.length + ' but should have been ' + oldContent.length - keys.length;
    }
  }
  catch(err) {
    handleError = cUseful.showError(err) + "(writing "+ self.getTableName()+" to memory)";
    handleCode =  enums.CODE.DRIVER;
  }
  
  return self.getParentHandler().makeResults (handleCode,handleError);
};


  