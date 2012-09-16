#!/usr/bin/env node

var dao_mysql = require( '../src/dao_mysql' );
var mug       = require( 'node-mug');
var log       = require( 'util' ).log;
var props     = require( 'node-props' );

var dao;

var date1;
var date2;

var dao_options;

log( "reading properties file" );

props.read( function ( p ) {
  dao_options = p.persistence;
  dao_options.populate = true;
  init_mug();
} );

function init_mug () {
  log( "initializing UUID generator" );

  mug.createInstance( function ( generator ) {
    dao_options.uuid = generator;

    log( "creating DAO" );
    create_dao();
  } );
}

function create_dao() {
    dao = new dao_mysql( dao_options );

    log( "initializing DAO" );
    dao.init( function ( err, data ) {
	if( err ) {
	    log( 'dao.init() err: ' + err.toString() );
	    dao.close( _closed );
	    process.exit( 2 );
	}

	setTimeout( function () { dao.close( _closed ); }, 1000 );

    } );
}

function _closed () {
    log( "DB connection closed" );
}
