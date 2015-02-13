/** wrapper
 */

function createDriver (handler,siloId,driverSpecific,driverOb, accessToken) {
    return new DriverSheet(handler,siloId,driverSpecific,driverOb, accessToken);
}


/**
 * DriverSheet
 * @param {cDataHandler} handler the datahandler thats calling me
 * @param {string} sheetName the name of the sheet the data is on
 * @param {string} ssId the workbook/sheet key
 * @return {object} result with handle or some error
 */
function getLibraryInfo () {
  return {
    info: {
      name:'cDriverSheet',
      version:'2.0.5',
      key:'Mrckbr9_w7PCphJtOzhzA_Cz3TLx7pV4j',
      description:'database abstraction driver for Google Sheets',
      url:'https://script.google.com/d/18fvqHqSs2YwU2ZMUcx6-9GE30u6i663rZTz7K0xNsStHoiJcs487JvN-/edit?usp=sharing'
    },
    dependencies:[
      cDriverMemory.getLibraryInfo(),
      cFlatten.getLibraryInfo(),
      cUseful.getLibraryInfo(),
      cDelegateMemory.getLibraryInfo()
    ]
  }; 
}

// this can be used for small datasets in place of a database
// a JSON object is stored as a Drive file and can be manipulated to provide database like characterisitcs
// data is stored as [ {key:someuniquekey, data:{},...]
 
var DriverSheet = function (handler,sheetName,ssId) {
  var siloId = sheetName;
  var self = this;
  var uniqueKey = ssId;
  var parentHandler = handler;
  var enums = parentHandler.getEnums();
  var ss;
  var errorMessage;
  var handleError, handleCode;
  var transactionBox_ = null;
  
  self.getType = function () {
    return enums.DB.SHEET;
  };
  
  // im able to do transactions
  self.transactionCapable = true;
  
  // i definitely need transaction locking
  self.lockingBypass = false;
  
  // i am aware of transactions and know about the locking i should do
  self.transactionAware = true;
  
  self.getDbId = function () {
    return uniqueKey;
  };
  
  self.getSiloId = function () {
    return siloId;
  };
  
  self.getParentHandler = function () {
    return parentHandler;
  };

 /**
  * begins transaction and store current content
  * @param {string} id transaction id
  */ 
  self.beginTransaction = function (id) {
    transactionBox_ = delegate.beginTransaction (id);
  };

  self.transactionData = function (){
    return delegate.transactionData();
  };

  /**
  * commits transaction
  * @param {string} id transaction id
  * @return {object} a normal result package
  */ 
  self.commitTransaction = function (id) {
    return delegate.commitTransaction(id);
  };
  
  self.clearTransactionBox = function () {
    transactionBox_ = null;
  };
 /**
  * roll back transaction - resets memory to beginnging of transaction
  * @param {string} id transaction id
  * @return {object} a normal result package
  */ 
  self.rollbackTransaction = function (id) {
    return delegate.rollbackTransaction(id);
  };


  self.appFlush = function () {
   
    parentHandler.rateLimitExpBackoff (function ( ) { 
      return SpreadsheetApp.flush(); 
    } ,  undefined ,  3 ) ;
    
  };
  
  self.createHandle = function() {

    try {

      return parentHandler.lock(self.getType()).protect ("creatingsheet", function (lock) {
        var ss = SpreadsheetApp.openById(uniqueKey);
        if (ss) {
          var r = ss.getSheetByName(siloId) || ss.insertSheet(siloId);
          self.appFlush();
          return r;
        }
        else {
          throw 'error opening spreadsheet' + uniqueKey;
        }
      }).result;

    }
    catch(err) {
     errorMessage = err;
     return null;
    }
  };

  var handle = self.createHandle();
  var delegate = new cDelegateMemory.DelegateMemory(self);
  
  self.getExceptionMessage = function () {
    return errorMessage;
  };
  
  self.getVersion = function () {
    var v = getLibraryInfo().info;
    return v.name + ':' + v.version;
  };
  
  self.getDriveHandle =  function () {
    return handle;
  };
  
  /**
   * DriverSheet.getTableName()
   * @return {string} table name or silo
   */
  self.getTableName = function () {
    return sheetName;
  };
  
 /**
  * extract the values from an array of objects
  * @param {Array.<String>} head an array of column headers
  * @param {Array.<Object>} flat an array of flattened objects
  * @return {Array.<Array>} an array of arrays of values
  */
  self.valueIfy = function (heads,  flat) {
  
    var headOb = self.makeHeadOb(heads);
    
    return flat.map (function(row) {
      var k = heads.map(function(d) { return '';});
      return Object.keys(row).reduce(function(p,c){
        p[headOb[c]] = row[c];
        return p;
      },k);
    });
    
  };    
  
  self.objectify = function (heads,  data) {
    
    var ob = {};
    
    data = data || [];
    data.forEach (function(d,i) {
      ob[heads[i]] = d;
    });
    

    return ob;
    
  };
  
 /** get the contents of the file
  * @return {Object} the parsed content of the file
  */
  self.getContent = function () {
    // get data and headings
    
    var contents = self.getSheetContents ();

    var obs = contents.values.map(function(row,r) {
      var ob = {};
      row.forEach (function(cell,c) {
          ob[contents.headings[c]] = cell;
      });
      
      return ({
        data:self.objectify(contents.headings,row)
      });
    
    });
    
    
    return obs.map(function(d) {
      return new cFlatten.Flattener().unFlatten(d.data);
    });;
  };
  
  
  /**
   * DriverSheet.getSheetContents()
   * @return {Array.<Array>} values on sheet
   */
  self.getSheetContents =function() {
    
    self.appFlush();
    
    // get all the data on the sheet
    var d = parentHandler.rateLimitExpBackoff ( function () { 
      return handle.getDataRange().getValues();
    });
    
    var contents = {};

    // the headings - you get wierd results for an empty row
    contents.headings = d && d.length && ((d[0].length  && d[0][0] !== '') || d.length > 1) ? d[0] : [];
        
    if (d && d.length > 1)  d.shift();
    contents.values = contents.headings.length && d && d.length ? d : [];
    
    return contents;
  };

  /**
   * DriverSheet.resizeSheet()
   * @param {number} nRows size of new sheet
   * @param {number} nCols size of new sheet
   * @return {DriverSheet} for chainging
   */
  self.resizeSheet = function (  nRows,nColumns) {
    
    
    // need to extend the sheets if necessary
    var howMany =  handle.getMaxRows() - nRows  ;
    if (howMany < 0 ) { 
      // lets save a bit of work by inserting more rows that we really need
      handle.insertRowsAfter(handle.getMaxRows(), Math.max( -howMany , enums.SETTINGS.EXTRAROWS));
    }
    var howMany = handle.getMaxColumns() - nColumns ;
    if (howMany < 0 ) { 
      handle.insertColumnsAfter(handle.getMaxColumns(), -howMany);
    }
    return self;
  };
  
  /**
   * --------------------------------
   * DriverSheet.generateHeadings ()
   * adds to current headings if new data has additional fields
   * @param {Array.string} heads current headings
   * @param {Array.Object} obs all the data
   * @return {Array.string} new Headings
   */
  self.addHeadings = function (heads, obs) {

    return  obs.reduce (function (p,c) {
      Object.keys(c).forEach (function(k) {
        if (p.indexOf(k) === -1) { 
          p.push(k);
          
        }
      });
      return p;
    } ,
    heads ? heads.slice() : []);
    
  };
 
  /**
   * --------------------------------
   * DriverSheet.generateHeadings ()
   * adds to current headings if new data has additional fields
   * @param {Array.string} heads current headings
   * @return {Object} headingOb with indexes
   */
  self.makeHeadOb = function (heads) {

    var ob = {};
    heads.forEach ( function (k,i) {
      ob[k]=i; 
    });
    
    return ob;
    
  };
  
  /**
   * --------------------------------
   * DriverSheet.replace ()
   * replaces current sheet with whats in memory
   * @param {DriverMemory} mem to be saved
   * @return {Object} headingOb with indexes
   */
  self.replaceWithMemory = function (mem) {
    
    try {
      // clear the contents
      handle.getDataRange().clearContent();
      
     
      // flatten the new data
      var newFlat = mem.takeContent().map(function(d) {
        return d ? new cFlatten.Flattener().flatten(d.data) : null;
      });
    
      // valuify
      var newHeads = self.addHeadings ([], newFlat);
      if (newHeads.length) {
        var newValues = self.valueIfy  (newHeads,  newFlat);
        newValues.unshift(newHeads);
        // make sure sheet is big enough to take the new values
        self.resizeSheet (newValues.length, newHeads.length);

        // write everything in one shot
        parentHandler.rateLimitExpBackoff ( function () { 
          handle.getRange (1 , 1, newValues.length, newHeads.length).setValues(newValues);
        });
      }
      self.appFlush();
      return parentHandler.makeResults(enums.CODE.OK);
    }
    catch (err) {
      self.appFlush();
      return parentHandler.makeResults(enums.CODE.DRIVER,cUseful.showError(err));
    }
  }
  /**
   * --------------------------------
   * DriverSheet.putBack ()
   * writes to sheet
   * @param {DriverMemory} mem to be saved
   * @param {boolean} optAppend whether to append
   * @return {Object} headingOb with indexes
   */
  self.putBack = function (mem,optAppend) {

    var code = enums.CODE.OK, err = '';
    append = optAppend || false;
    
    var newContent = mem.takeContent();

    var sheetContents = self.getSheetContents();
    var dr = handle.getDataRange();
    
    if (!newContent.length ) {
      
      // deleting the whole thing
      if (!append) {
        parentHandler.rateLimitExpBackoff ( function () { 
          if(dr.getNumRows() > 0) { 
            dr.clearContent();
          }
        });
      }
      // if appending, there's nothing to append
    
    }
    else {

      // have to write the new content and delete everything afterwards
      var newFlat = newContent.map(function(d) {
        return d ? new cFlatten.Flattener().flatten(d.data) : null;
      });

      var newHeads = self.addHeadings (sheetContents.headings, newFlat);
      var newValues = self.valueIfy  (newHeads,  newFlat);

      // data ends here
      var size = sheetContents.values.length +1 ;
      
      // so any appending starts here
      var startPoint = append ? size +1: 2;
      
      // make sure sheet is big enough to take the new values
      self.resizeSheet (startPoint+newValues.length, newHeads.length);
      
      // if any headings have changed
      self.updateHeaders (sheetContents.headings, newHeads);

      // now write the new data
      parentHandler.rateLimitExpBackoff ( function () { 
         handle.getRange (startPoint , 1,newValues.length, newHeads.length).setValues(newValues);
      });
     
      // and delete the rest
      if (size > newValues.length + startPoint-1 && !append) {

        parentHandler.rateLimitExpBackoff ( function () { 
           return handle.deleteRows(startPoint + newValues.length ,size - (startPoint+ newValues.length -1)); 
        });
      }
      
    }
    self.appFlush();
    
    return parentHandler.makeResults(code,err);

  };
  
 /**
  * updates headers in spreadsheet if necessary
  * @param {Array.<String>} oldHeads an array of the old  column titles
  * @param {Array.<String>} newHeads  an array of the new  column titles
  * @return {Void}
  */
  self.updateHeaders = function(oldHeads,newHeads) {
  
        // if any headings have changed
    if(parentHandler.checksum(newHeads) != parentHandler.checksum(oldHeads) || oldHeads.length != newHeads.length) {
      parentHandler.rateLimitExpBackoff ( function () { 
        handle.getRange (1,1,1,newHeads.length).setValues([newHeads]);
      });
    }
  
  };
  
  
 /**
  * gets content from spreadsheet
  * @param {DriverMemory} mem a memory driver object
  * @return {DriverMemory} mem the memory driver object
  */
  self.take = function (mem) {
    mem.save ( self.getContent() || [] );
    return mem;
  }; 
  
  self.unFlatten = function (data) {
    return data.map (function(d) {
      return new cFlatten.Flattener().unFlatten (d);
    });
  };
  
  self.flatten = function (data) {
    return data.map (function(d) {
      return new cFlatten.Flattener().flatten (d);
    });
  };
  
  /**
   * DriverSheet.query()
   * @param {object} queryOb some query object 
   * @param {object} queryParams additional query parameters (if available)
   * @param {boolean} keepIds whether or not to keep driver specifc ids in the results
   * @return {object} results from selected handler
   */
  self.query = function (queryOb,queryParams,keepIds) {
    return delegate.query(queryOb,queryParams,keepIds);
  };

  self.getTransactionBox = function () {
    return transactionBox_;
  };
  
  /**
   * DriverSheet.remove()
   * @param {object} queryOb some query object 
   * @param {object} queryParams additional query parameters (if available)
   * @return {object} results from selected handler
   */
  self.remove = function (queryOb,queryParams) {
    return delegate.remove(queryOb,queryParams);
  };
   
 /**
  * DriverSheet.save()
  * @param {Array.object} obs array of objects to write
  * @return {object} results from selected handler
  */ 
  self.save = function (obs) {
    return delegate.save(self.unFlatten(obs));
  };

  /**
   * get the memory - if its a transaction we already have it, if not read the sheet and make one
   * @return {DriverMemory} the men object
   */
  self.getMem = function () {
       
    return parentHandler.inTransaction() ? 
      transactionBox_.content :
      self.take(new cDriverMemory.DriverMemory(parentHandler, siloId)) ;
  
  };
  
  /**
   * DriverSheet.count()
   * @param {object} queryOb some query object 
   * @param {object} queryParams additional query parameters (if available)
   * @return {object} results from selected handler
   */
  self.count = function (queryOb,queryParams) {
    return delegate.count(queryOb,queryParams);
  };

  /**
   * Driver.get()
   * @param {Array.object} keys the unique return in handleKeys for this object
   * @return {object} results from selected handler
   */
  self.get = function (keys) {
    return delegate.get(keys);
  };

  /**
   * getGuts_()
   * @param {Array.object} keys the unique return in handleKeys for this object
   * @return {object} results from selected handler
   */
  self.getGuts = function (keys) {

     if (!self.isTransaction(parentHandler.getTransactionId())) {
       var contents = self.getSheetContents();

       // get specific rows
       var selected = self.unFlatten(keys.map(function(d) {
         return self.objectify(contents.headings,contents.values[d.row-1]);
       }));
       
       
       // check they havent changed
       if (selected.every (function (d,i) {
         return cUseful.checksum(d) === keys[i].checksum; 
       })) {
         return {results:parentHandler.makeResults(enums.CODE.OK,'',selected),contents:contents};
       }
       else {
         return {results:parentHandler.makeResults(enums.CODE.CHECKSUM,'get failed'),contents:contents}; 
       }
    }
    else {

      return {results:transactionBox_.content.get( keys,'key'),contents:null};
    }

  };

  /**
   * Driver.update()
   * @param {Array.string} keys the unique return in handleKeys for this object
   * @param {object} obs what to update it to
   * @return {object} results from selected handler
   */
  self.update = function (keys,obs) {
    return delegate.update (keys,obs);
  };

  self.updateGuts = function (keys,obs) {
      
    // get the data
    var result = self.getGuts(keys);
    
    if (result.results.handleCode >=0) {
      
      var memory = new cDriverMemory.DriverMemory(parentHandler, siloId);
      var r = memory.save(obs);
      
      // write it back
      if (r.handleCode >=0) {
        var newContent = memory.takeContent();
        
        var newFlat = newContent.map(function(d) {
          return new cFlatten.Flattener().flatten(d.data? d.data : d);
        });
        
        
        var newHeads = self.addHeadings (result.contents.headings, newFlat);
        var newValues = self.valueIfy  (newHeads,  newFlat);
        
        // if any headings have changed
        self.updateHeaders (result.contents.headings, newHeads);
        
        // now write the new data
        newValues.forEach (function (d,i) {
          parentHandler.rateLimitExpBackoff ( function () { 
            handle.getRange (keys[i].row +1 ,1, 1,d.length).setValues([d]);
          })
        });
        
        self.appFlush(); 
      }
      result = r;
    }
    
    return result;

  };
  
  
  return self;
  
}
