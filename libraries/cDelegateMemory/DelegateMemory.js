"use strict";

/**
 * DelegateMemory
 * @param {object} delegator the driver calling me
 * @return {DelegateMemory} self
 */
function getLibraryInfo () {
  return {
    info: {
      name:'cDelegateMemory',
      version:'2.0.1',
      key:'MyIN8WHN1Uf3EG-obHsjrAyz3TLx7pV4j',
      description:'database delegation abstraction driver',
      url:'https://script.google.com/d/1vTqRouwf8VVyz9lSdqMBhfuqUM0po3GQCwfjbTlCqOKB2QjGAFbum0dL/edit?usp=sharing'
    },
    dependencies:[
      cDriverMemory.getLibraryInfo()
    ]
  }; 
}

// this can be used for small datasets in place of a database
// a JSON object is stored as a Drive file and can be manipulated to provide database like characterisitcs
// data is stored as [ {key:someuniquekey, data:{},...]
 
var DelegateMemory = function (delegator) {

  var self = this;
  var driver_ = delegator;
  var parentHandler_ = driver_.getParentHandler();
  var enums_ = parentHandler_.getEnums();

  
  /**
   * DelegateMemory.query()
   * @param {object} queryOb some query object 
   * @param {object} queryParams additional query parameters (if available)
   * @param {boolean} keepIds whether or not to keep driver specifc ids in the results
   * @return {object} results from selected handler
   */
  self.query = function (queryOb,queryParams,keepIds) {
  
    return parentHandler_.readGuts('query', 
      function (bypass) {
        return driver_.getMem().query ( queryOb,queryParams,keepIds);
      });
      
  };

  
  /**
   * DelegateMemory.remove()
   * @param {object} queryOb some query object 
   * @param {object} queryParams additional query parameters (if available)
   * @return {object} results from selected handler
   */
  self.remove = function (queryOb,queryParams) {
  
    return parentHandler_.writeGuts('remove', 
      
      function (bypass) {
        var mem = driver_.getMem();
        var r = mem.remove (queryOb,queryParams,'key');
        return r.handleCode >=0 ? driver_.putBack (mem) : r;
      },
      
      function (bypass) {
        driver_.getTransactionBox().dirty = true;
        return driver_.getTransactionBox().content.remove(queryOb,queryParams,'key');
      });
      
  };
   
 /**
  * DelegateMemory.save()
  * @param {Array.object} obs array of objects to write
  * @return {object} results from selected handler
  */ 
  self.save = function (obs,mem) {


    return parentHandler_.writeGuts('save', 
      
      function (bypass) {
        // we need to append these obs
        var memory = mem || new cDriverMemory.DriverMemory(parentHandler_, driver_.getSiloId());
        var r = memory.save(obs);
        if (r.handleCode <0) return r;

        // write this using the appropriate driver
        var p = driver_.putBack (memory,true);
        
        // now return the result of the writeback, along with the keys of the newly inserted items
        return parentHandler_.makeResults (p.handleCode, p.handleError , obs ,undefined, r.handleKeys);
      },
      
      function (bypass) {
        driver_.getTransactionBox().dirty = true;
        return driver_.getTransactionBox().content.save (obs);
      });
      

  };

  /**
   * DelegateMemory.count()
   * @param {object} queryOb some query object 
   * @param {object} queryParams additional query parameters (if available)
   * @return {object} results from selected handler
   */
  self.count = function (queryOb,queryParams) {
  
  
    return parentHandler_.readGuts('count', 
      function (bypass) {
        return driver_.getMem().count ( queryOb,queryParams) ;
      });
      
  };

  /**
   * driver_.get()
   * @param {Array.object} keys the unique return in handleKeys for this object
   * @return {object} results from selected handler
   */
  self.get = function (keys) {
  
    return parentHandler_.readGuts('get', 
      function (bypass) {
        return driver_.getGuts(keys).results;
      },
      function (bypass) {
        return  driver_.getMem().get(keys)
      });

  };

   /**
   * driver_.update()
   * @param {Array.string} keys the unique return in handleKeys for this object
   * @param {object} obs what to update it to
   * @return {object} results from selected handler
   */
  self.update = function (keys,obs) {

    return parentHandler_.writeGuts('update', 
      function (bypass) {
        return driver_.updateGuts (keys,obs);
      },
      function (bypass) {
        driver_.getTransactionBox().dirty = true;
        return driver_.getTransactionBox().content.update (keys,obs,'key');
      });

    
  };
 
 /**
  * begins transaction and store current content
  * @param {string} id transaction id
  */ 
  self.beginTransaction = function (id) {

    return {
      id: id,
      content: null,
      dirty: false
    };
  };
  
  self.transactionData = function () {
    var memory = new cDriverMemory.DriverMemory(parentHandler_, driver_.getSiloId());
    driver_.getTransactionBox().content = driver_.take(memory);
    return parentHandler_.makeResults (enums_.CODE.OK);
  };
 /**
  * commits transaction
  * @param {string} id transaction id
  * @return {object} a normal result package
  */ 
  self.commitTransaction = function (id) {
    Logger.log('committing');
    // double check we are committing the correct transaction
    if (!parentHandler_.isTransaction (id)) {
      return parentHandler_.makeResults (enums_.CODE.TRANSACTION_ID_MISMATCH);
    }
    
    // commit current memory state to replace current contents of sheet.
    if (driver_.getTransactionBox().dirty) {
      var result = driver_.replaceWithMemory(driver_.getTransactionBox().content);
    }
    else {
      var result = parentHandler_.makeResults(enums_.CODE.OK);
    }
    if (result.handleCode >=0) {
      driver_.clearTransactionBox();
    }
    return result;
  };
  
 /**
  * roll back transaction - resets memory to beginnging of transaction
  * @param {string} id transaction id
  * @return {object} a normal result package
  */ 
  self.rollbackTransaction = function (id) {
    
    // double check we are committing the correct transaction
    if (!parentHandler_.isTransaction (id)) {
      return parentHandler_.makeResults (enums_.CODE.TRANSACTION_ID_MISMATCH);
    }
    
    // nothing to do here - we didnt write anything yet anyway
    driver_.clearTransactionBox();
    return parentHandler_.makeResults(enums_.CODE.OK);
  };
  
   /**
    * put back where we are simplifying the key
    * @param {cDriverMemory} mem a memory object
    * @return {object} a normal result package
    */

  self.putBackSimpleKeys = function (mem) {
    
    try {
      parentHandler_.rateLimitExpBackoff ( function () { 
        return driver_.writeContent(mem.takeContent().map (function (d) {
          // compatibility with previous version
          return {data:d.data , key: d.keys ? d.keys.key : d.key};
        }));
      });
      return parentHandler_.makeResults(enums_.CODE.OK);
    }
    catch (err) {
      return parentHandler_.makeResults(enums_.CODE.DRIVER,err);
    }

  };
 

 /** get the contents of the property
  * @return {object} the parsed content of the file
  */
  self.getContentSimpleKeys = function (content) {

    var memory = new cDriverMemory.DriverMemory(parentHandler_, driver_.getSiloId());
    // generate the keys
    var keyed =  content ? content.map (function (d,i) {
            var k = memory.makeMemoryEntry (d.data,i);
            // retain the original key if there was one
            if (d.keys) {
              k.keys.key = d.keys.key;
            }
            return k;
          }) : null;
    
    return keyed;
  };
  
  
  
  return self;
  
}
