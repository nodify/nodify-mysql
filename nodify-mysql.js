( function ( ) {
  var mysql = require( 'mysql' );
  var fs    = require( 'fs' );

  var log;

  function nodify_mysql ( o ) {
    this.host = o.host;
    this.user = o.user;
    this.pass = o.pass;
    this.db   = o.db;
    this.uuid = o.uuid;
    this.descriptorPath = o.descriptorPath;
    this.populate = o.populate;

    if( this.log ) {
      log = this.log;
    } else {
      log = require( 'util' ).log;
    }
  }

  if( module && module.exports ) {
    module.exports = nodify_mysql;
  }

  nodify_mysql.prototype.init = function ( complete ) {
    var that = this;

    this.descriptor = JSON.parse ( fs.readFileSync( this.descriptorPath, 'utf-8' ) );

    this.createClient();

    if( this.populate ) {
      return deleteExistingDatabase.apply( this, [this.descriptor ] );
    } else {
      return read_database.apply( this, [ this.descriptor ] );
    }

    function deleteExistingDatabase( d ) {
      that.do_query( 'USE mysql' );
      that.do_query( 'DROP DATABASE IF EXISTS ' + that.db );
      that.do_query( 'CREATE DATABASE ' + that.db );
      that.do_query( 'USE ' + that.db );
      read_database.apply( that, [d] );
    }

    function read_database ( d ) {

      for( var item in d ) {
    	var current = d[ item ];

    	_create_accessors( item, current );

	if( this.populate ) {		    
    	  _create_table( item, current );
    	  _populate_table( item, current.insert );
	}
      }

      complete( null, this );
    }

    function _populate_keys ( fields ) {
      var keys = [];

      for( var item in fields ) {
    	keys.push( item );
      }

      keys.sort();

      return( keys );
    }

    function _fields ( keys, source ) {
      var fields = [];

      for( var i = 0, il = keys.length; i < il; i++ ) {
    	fields.push( source[ keys[i] ] );
      }

      return( fields );
    }

    function _create_table( name, data ) {
      var create = "CREATE TABLE " + name + "( ";
      var fields = [];

      for( var item in data.items ) {
    	var current = data.items[ item ];
    	var current_field = item + " " + current;
    	if( item === data.key ) {
    	  current_field += " KEY";
    	}
    	fields.push( current_field );
      }

      if( data.created ) {
        fields.push( 'created TIMESTAMP DEFAULT CURRENT_TIMESTAMP' );
      } else if( data.updated ) {
        fields.push( 'updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP' );
      }

      if( data.expires ) {
        fields.push( "expires TIMESTAMP" );
      }

      create += fields.join(',') + ")";

      that.do_query( create );

      if( ! data.key ) {
	that.do_query( 'INSERT INTO ' + name + ' () VALUES ()' );
      }
    }

    function _create_accessors( name, fields ) {
      if( name && fields ) {
    	_populate_keys( fields );
    	fields.key && _create_create_accessor( name, fields );
    	_create_read_accessor( name, fields );
    	_create_update_accessor( name, fields );
    	fields.key && _create_delete_accessor( name, fields );
	fields.select && _create_by_accessors( name, fields );
        fields.query && _create_query_accessors( name, fields.query );
      }
      
      function _create_query_accessors ( item, queries ) {
        function _name( i, n ) {
          return( i + n );
        }

        function _func( n, q ) {
          return function( params, complete ) {
            that.do_query( q, params, complete );
          }
        }

        for( var i in queries ) {
          that[ _name( item, i ) ] = _func( i, queries[i].query );
        }
      }

      function _expires_clause( fields ) {
        if( fields.expires ) {
          return( " AND expires > '" + new Date( Date.now() ).toISOString() + "'");
        } else {
          return( '' );
        }
      }

      function _create_by_accessors ( name, fields ) {
        function _build_from_string ( name, item ) {
	  var query = 'SELECT * FROM ' + name + ' WHERE ' + item + '=?';
          query += _expires_clause( fields );

          return function ( id, complete ) {
    	    that.do_query( query, [id], complete );
	  };
        }

        function _build_from_array ( name, items ) {
          var query = 'SELECT * FROM ' + name + ' WHERE ';

          for( var i = 0, il = items.length; i < il; i++ ) {
            var current = items[i];
            query += current + '=?';
            if( i !== ( il - 1 ) ) {
              query += ' AND ';
            }
          }

          query += _expires_clause( fields );

          return function ( id, complete ) {
    	    that.do_query( query, id, complete );
	  };
        }

	for( var item in fields.select ) {
          if( 'string' === typeof fields.select[ item ] ) {
	    that[ name + 'ReadBy' + item.substring( 0, 1 ).toUpperCase() + item.substring( 1 ) ] = _build_from_string( name, item );
          } else if( fields.select[item].length ) {
            var index = name + 'ReadBy';

            for( var i = 0, il = fields.select[item].length; i < il; i++ ) {
              var current = fields.select[item][i];
              index += current.substring( 0, 1 ).toUpperCase() + current.substring( 1 );
            }

            that[ index ] = _build_from_array( name, fields.select[item] );
          }
	}
      }

      function _create_create_accessor( name, fields ) {
    	that[ name + 'Create' ] = function( source, complete ) {
    	  var keys;
    	  var query;

    	  if( source[ fields.key ] ) {
    	    _insert( source[ fields.key ] );
    	  } else {
    	    that.uuid.generate( _insert );
    	  }

    	  function _insert( u ) {
    	    if( u ) {
    	      source[ fields.key ] = u.toString();
    	    }

    	    keys = _populate_keys( source );
            
            if( fields.expires && ! source.expires ) {
              keys.push( 'expires' );
              source.expires = new Date( Date.now() + 86400000 );
            }

    	    query = 'INSERT INTO ' + name + ' SET ' + keys.join( '=?,' ) + '=?';

    	    that.do_query( query, _fields( keys, source ), _err( _finish ) );

    	    function _finish( err, info ) {
    	      complete( null, source );
    	    }
    	  }
    	};
      }

      function _create_read_accessor( name, fields ) {
    	that[ name + 'Read' ] = function ( id, complete ) {
	  if( fields.key ) {
	    var query = 'SELECT * FROM ' + name + ' WHERE ' + fields.key + '=?';
            query += _expires_clause( fields );
    	    that.do_query( query, [id], _err( _final ) );
	  } else {
	    that.do_query( 'SELECT * FROM ' + name, [], _err( _final ) );
	  }

    	  function _final( err, data ) {
	    if( data && ( data.length > 0 ) ) {
    	      complete( err, data );
	    } else {
	      complete( err, [] );
	    }
    	  }
    	};
      }

      function _create_update_accessor( name, fields ) {
    	that[ name + 'Update' ] = function( source, complete ) {
    	  var keys;
    	  var flist;
          var q;

    	  keys = _populate_keys( source );
    	  flist = _fields( keys, source );

    	  if( source[ fields.key ] ) {
    	    flist.push( source[ fields.key ] );
            q = 'UPDATE ' + name + ' SET ' + keys.join( '=?,' ) + '=? WHERE ' + fields.key + '=?';
            
    	  } else {
            q = 'UPDATE ' + name + ' SET ' + keys.join( '=?,' ) + '=?';
    	  }

    	  that.do_query( q, flist, _err( _final ) );

    	  function _final( err, info ) {
    	    complete( null, source );
    	  }
    	};
      }

      function _create_delete_accessor( name, fields ) {
    	that[ name + 'Delete' ] = function( id, complete ) {
    	  that.do_query( 'DELETE FROM ' + name + ' WHERE ' + fields.key + '=?', [id], _final );

    	  function _final( err, info ) {
    	    complete( err, null );
    	  }
    	};
      }
    }
    
    function _populate_table (name, fields) {
      var f = that[ name + 'Create' ] || that[ name + 'Update' ];

      if( fields && f ) {
    	for( var i = 0, il = fields.length; i < il; i++ ) {
    	  f( fields[i], _f( null ) );
    	}
      }
    }

    return( this );
  };

  nodify_mysql.prototype.do_query = function ( query, params, complete ) {
    try {
      this.connection.query( query, params, complete );
    } catch( e ) {
      console.log( 'QUERY: ' + JSON.stringify( e ) ); 
    }
  };

  nodify_mysql.prototype.close = function ( complete ) {
    this.connection.end( complete );
  };
  
  nodify_mysql.prototype.createClient = function( ) {
    var that = this;

    this.connection = mysql.createConnection( {
      host: this.host,
      user: this.user,
      password: this.pass
    } );

    this.connection.on( 'error', function( err ) {
      console.log( 'CONNECT: ' + JSON.stringify( err ) );
      if( ! err.fatal ) {
        return;
      }

      if( err.code !== 'PROTOCOL_CONNECTION_LOST' ) {
        console.log( 'err is not PROTOCOL_CONNECTION_LOST: ' + err.code );
        throw err;
      }

      that.createClient();
    } );


    this.connection.connect();

    this.do_query( 'USE ' + this.db );
  };

  function _safe( f ) {
    return function ( id, complete ) {
      try {
    	return f.apply( this, [ id, complete ] );
      } catch( e ) {
    	return complete( e.toString(), null );
      }
    };
  }

  function _err( f ) {
    return function( err, data ) {
      if( err ) {
    	return f.apply( this, [ err.toString(), null ] );
      } else {
    	return f( null, data );
      }
    };
  }

  function _f ( f ) {
    if( f ) {
      return f;
    } else {
      return function () {};
    }
  }

} ) ( );