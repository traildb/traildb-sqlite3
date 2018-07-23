#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include <traildb.h>
#include <string.h>

struct sqlite3_vtab_traildb {
    struct sqlite3_vtab vtab;
    tdb* traildb_handle;
};

struct sqlite3_vtab_cursor_traildb {
    struct sqlite3_vtab_traildb* vtab;
    tdb* traildb_handle;
    tdb_cursor* cursor;
    const tdb_event* event;
    int empty_traildb;
    uint64_t row_idx;
    uint64_t trail_id;
    uint64_t total_trails;
};

static int traildbNext(sqlite3_vtab_cursor* cur)
{
    // This function is called when we have to go to next row.
    struct sqlite3_vtab_cursor_traildb* scur = (struct sqlite3_vtab_cursor_traildb*) cur;

    if ( scur->empty_traildb || scur->trail_id >= scur->total_trails ) {
        // Stop here if we are at the end of TrailDB.
        return SQLITE_OK;
    }

    do {
        // In this do-while loop, we won't exit unless TrailDB is at EOF
        // or we got next event to scur->event.
        scur->event = tdb_cursor_next(scur->cursor);
        if ( !scur->event ) {
            // If there are no more events in cursor, step the cursor to next trail.
            scur->trail_id++;
            if ( scur->trail_id < scur->total_trails ) {
                tdb_error err = tdb_get_trail(scur->cursor, scur->trail_id);
                if ( err != TDB_ERR_OK ) {
                    return SQLITE_ERROR;
                }
            }
        }
    }
    while ( scur->trail_id < scur->total_trails && scur->event == 0 );
    // Increment row id (sqlite3 thing, it wants a unique row id for every row)
    scur->row_idx++;

    return SQLITE_OK;
}

static int traildbOpenCursor(sqlite3_vtab *vtab, sqlite3_vtab_cursor** cur)
{
    // This function is called when sqlite3 wants to set up a cursor.
    // It actually maps pretty nicely to TrailDB cursor.
    struct sqlite3_vtab_traildb* svt = (struct sqlite3_vtab_traildb*) vtab;
    (*cur) = sqlite3_malloc(sizeof(struct sqlite3_vtab_cursor_traildb));
    if ( !(*cur) ) {
        return SQLITE_NOMEM;
    }
    memset( (*cur), 0, sizeof(struct sqlite3_vtab_cursor_traildb) );
    struct sqlite3_vtab_cursor_traildb* scur = (struct sqlite3_vtab_cursor_traildb*) (*cur);

    // Make internal TrailDB cursor.
    tdb_cursor* cursor = tdb_cursor_new(svt->traildb_handle);
    if ( !cursor ) {
        sqlite3_free( scur );
        (*cur) = 0;
        return SQLITE_NOMEM;
    }

    // How many trails do we have in total? If there are no trails, set a
    // special flag.
    scur->cursor = cursor;
    if ( tdb_num_trails(svt->traildb_handle) == 0 ) {
        scur->empty_traildb = 1;
    } else {
        tdb_error err = tdb_get_trail(cursor, 0);
        if ( err != TDB_ERR_OK ) {
            sqlite3_free( scur );
            (*cur) = 0;
            return SQLITE_NOMEM;
        }
    }

    scur->traildb_handle = svt->traildb_handle;
    scur->total_trails = tdb_num_trails(svt->traildb_handle);

    return SQLITE_OK;
}

static int traildbEof(sqlite3_vtab_cursor* cursor)
{
    // This function is called by sqlite3 to test if the cursor is exhausted.
    struct sqlite3_vtab_cursor_traildb* cur = (struct sqlite3_vtab_cursor_traildb*) cursor;
    if ( cur->empty_traildb || cur->trail_id >= cur->total_trails ) {
        return 1;
    }
    return 0;
}

static int traildbBestIndex(sqlite3_vtab* vtab, sqlite3_index_info* info)
{
    // This function can be used to set up indexes to help query planner.
    //
    // The thing we could do is match by UUID to only get a certain UUID.
    // Right now we only fill estimated rows and basically Sqlite3 will almost
    // always just scan everything.
    //
    // Other ideas:
    // For equality checks we could do a quick check if the lexicon contains a value.
    // If lexicon does not contain the value, we know we can skip everything.
    //
    // TODO: implement a filter that detects if query says something like
    // UUID=<some value> and then only step on that UUID specifically.
    struct sqlite3_vtab_traildb* svt = (struct sqlite3_vtab_traildb*) vtab;
    info->estimatedRows = tdb_num_events(svt->traildb_handle);
    info->estimatedCost = tdb_num_events(svt->traildb_handle);
    return SQLITE_OK;
}


static int traildbFilter(sqlite3_vtab_cursor* cursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv)
{
    // This function is called when sqlite3 wants to start a new query.
    //
    // This function is supposed to use idxNum and idxStr with traildbBestIndex
    // to figure out how to actually run the query.
    //
    // Our queries are all just scans so we don't use them.
    struct sqlite3_vtab_cursor_traildb* cur = (struct sqlite3_vtab_cursor_traildb*) cursor;
    if ( cur->empty_traildb ) {
        return SQLITE_OK;
    }

    tdb_error err = tdb_get_trail(cur->cursor, 0);
    if ( err != TDB_ERR_OK ) {
        return SQLITE_NOMEM;
    }

    int rc = traildbNext(cursor);
    cur->row_idx = 0;
    return rc;
}

static int traildbCloseCursor(sqlite3_vtab_cursor* cursor)
{
    struct sqlite3_vtab_cursor_traildb* svct = (struct sqlite3_vtab_cursor_traildb*) cursor;
    if ( svct->cursor ) {
        tdb_cursor_free(svct->cursor);
        svct->cursor = 0;
    }
    sqlite3_free(svct);
    return SQLITE_OK;
}

// This function comes from zipfile module; renamed for TrailDB
// Its purpose is to remove quotes in TrailDB filename, e.g.
//
// CREATE VIRTUAL TABLE mytdb USING tdb ("./traildb");
//
// This function removes the double quotes in "./traildb". If we don't
// do this we actually try to find a file named "./traildb" (including the
// double quotes in the filename).
static void fileDequote(char *zIn){
    char q = zIn[0];
    if( q=='"' || q=='\'' || q=='`' || q=='[' ){
        int iIn = 1;
        int iOut = 0;
        if( q=='[' ) q = ']';
        while(1){
            char c = zIn[iIn++];
            if( c==q && zIn[iIn++]!=q ) break;
            zIn[iOut++] = c;
        }
        zIn[iOut] = '\0';
    }
}

static int traildbConnect(sqlite3 *db,
                          void* aux,
                          int argc,
                          const char *const *argv,
                          sqlite3_vtab **vtabs,
                          char** err)
{
    // We want exactly 4 arguments.
    // 0 = module name ("traildb")
    // 1 = database name (we don't care about it)
    // 2 = table name (we also don't care about this)
    // 3 = first argument (this is what we care about, it's the name of the traildb)
    if ( argc < 4 || argc > 4 ) {
        *err = sqlite3_mprintf("traildb requires one argument exactly");
        return SQLITE_ERROR;
    }

    // Set up TrailDB handles
    tdb* t = tdb_init();
    if ( !t ) {
        *err = sqlite3_mprintf("traildb failed to call tdb_init()");
        return SQLITE_ERROR;
    }

    char* fname = sqlite3_malloc(strlen(argv[3])+1);
    if ( !fname ) {
        *err = sqlite3_mprintf("out of memory");
        return SQLITE_NOMEM;
    }
    memcpy(fname, argv[3], strlen(argv[3]));
    fileDequote(fname);

    tdb_error terr = tdb_open(t, fname);
    if ( terr != TDB_ERR_OK ) {
        *err = sqlite3_mprintf("traildb failed to call tdb_open(%s): %s", fname, tdb_error_str(terr));
        sqlite3_free(fname);
        return SQLITE_ERROR;
    }
    sqlite3_free(fname);

    // We have to do a little dance to produce this CREATE TABLE line sqlite3 wants.
    //
    // CREATE TABLE v(uuid, timestamp, ...);
    //
    // ^ This is the string we have to create. Table name doesn't matter. Most
    // hoops are in the last part.
    //
    // Allocate an array of pointers to C strings.
    size_t total_length_of_fields = 0;
    char** traildb_fields_str = sqlite3_malloc(sizeof(char*) * (tdb_num_fields(t)+1)); // uuid + all other fields
    if ( !traildb_fields_str ) {
        tdb_close(t);
        return SQLITE_NOMEM;
    }
    memset(traildb_fields_str, 0, (tdb_num_fields(t)+1)*sizeof(char*));

    // Set UUID to first field.
    traildb_fields_str[0] = sqlite3_mprintf("uuid");
    total_length_of_fields += 6;
    if ( !traildb_fields_str[0] ) {
        sqlite3_free(traildb_fields_str);
        tdb_close(t);
        return SQLITE_NOMEM;
    }

    // Set timestamp to second field.
    traildb_fields_str[1] = sqlite3_mprintf("timestamp");
    total_length_of_fields += 11;
    if ( !traildb_fields_str[1] ) {
        sqlite3_free(traildb_fields_str[0]);
        sqlite3_free(traildb_fields_str);
        tdb_close(t);
        return SQLITE_NOMEM;
    }

    // Using TrailDB bindings, get the field names inside TrailDB and put them in our list.
    for ( uint64_t i = 0; i < tdb_num_fields(t)-1; ++i ) {
        traildb_fields_str[i+2] = sqlite3_mprintf("%s", tdb_get_field_name(t, i+1));
        if ( !traildb_fields_str[i+2] ) {
            for ( i = 0; i < tdb_num_fields(t)+1; ++i ) {
                if ( traildb_fields_str[i] ) {
                    sqlite3_free(traildb_fields_str[i]);
                }
            }
            sqlite3_free(traildb_fields_str);
            tdb_close(t);
            return SQLITE_NOMEM;
        }
        total_length_of_fields += strlen(traildb_fields_str[i+2])+2;
    }

    // Allocate a string that holds comma-separated list of fields.
    char* concatfields = sqlite3_malloc(total_length_of_fields);
    if ( !concatfields ) {
        for ( uint64_t i = 0; i < tdb_num_fields(t)+1; ++i ) {
            if ( traildb_fields_str[i] ) {
                sqlite3_free(traildb_fields_str[i]);
            }
        }
        tdb_close(t);
        return SQLITE_NOMEM;
    }

    memset(concatfields, 0, total_length_of_fields);

    uint64_t concat_cursor = 0;
    // Fill concatfields with the fields.
    for ( uint64_t i = 2; i < tdb_num_fields(t)+1; ++i ) {
        if ( i > 2 ) {
            // Put comma in-between fields
            concatfields[concat_cursor] = ',';
            concat_cursor++;
        }
        memcpy(&concatfields[concat_cursor], traildb_fields_str[i], strlen(traildb_fields_str[i]));
        concat_cursor += strlen(traildb_fields_str[i]);
    }

    // No longer need field strings themselves, free them up.
    for ( uint64_t i = 0; i < tdb_num_fields(t)+1; ++i ) {
        if ( traildb_fields_str[i] ) {
            sqlite3_free(traildb_fields_str[i]);
        }
    }
    sqlite3_free(traildb_fields_str);

    // Finally build the actual create table command.
    char* traildb_fields = sqlite3_mprintf("CREATE TABLE t( uuid TEXT, timestamp INTEGER, %s );", concatfields);
    sqlite3_free(concatfields);
    if ( !traildb_fields ) {
        tdb_close(t);
        return SQLITE_NOMEM;
    }

    // Declare the table. Sqlite3 demands it.
    int rc = sqlite3_declare_vtab(db, traildb_fields);
    sqlite3_free(traildb_fields);
    if ( rc != SQLITE_OK ) {
        tdb_close(t);
        return rc;
    }

    // Allocate sqlite3_vtab structure to keep track of our stuff.
    struct sqlite3_vtab_traildb* svt = sqlite3_malloc(sizeof(struct sqlite3_vtab_traildb));
    if ( !svt ) {
        tdb_close(t);
        return SQLITE_NOMEM;
    }
    memset(svt, 0, sizeof(struct sqlite3_vtab_traildb));

    svt->traildb_handle = t;

    (*vtabs) = (sqlite3_vtab*) svt;
    return SQLITE_OK;
}

static int traildbDisconnect(sqlite3_vtab *vtab)
{
    // Close TrailDB if needed.
    struct sqlite3_vtab_traildb* svt = (struct sqlite3_vtab_traildb*) vtab;
    if ( svt->traildb_handle ) {
        tdb_close(svt->traildb_handle);
        svt->traildb_handle = 0;
    }
    // Clean up all the things.
    sqlite3_free(vtab);
    return SQLITE_OK;
};

static int traildbColumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int N)
{
    struct sqlite3_vtab_cursor_traildb* scur = (struct sqlite3_vtab_cursor_traildb*) cur;
    // If there is no event, error out.
    // I think this is not possible to happen with sqlite3.
    if ( !scur->event ) {
        return SQLITE_ERROR;
    }

    // N=0 UUID column
    if ( N == 0 ) {
        const uint8_t* uuid = tdb_get_uuid(scur->traildb_handle, scur->trail_id);
        uint8_t hex_uuid[32];
        tdb_uuid_hex(uuid, hex_uuid);
        sqlite3_result_text(ctx, (const char*) hex_uuid, 32, SQLITE_TRANSIENT);
        return SQLITE_OK;
    }
    // N=1 Timestamp column
    if ( N == 1 ) {
        sqlite3_result_int64(ctx, (sqlite3_int64) scur->event->timestamp);
        return SQLITE_OK;
    }
    // Any other field, they are in N > 2
    uint64_t len = 0;
    const char* field_str = tdb_get_item_value(scur->traildb_handle, scur->event->items[N-2], &len);

    sqlite3_result_text(ctx, field_str, (int) len, SQLITE_TRANSIENT);
    return SQLITE_OK;
}

static int traildbRowId(sqlite3_vtab_cursor* cur, sqlite_int64* row_id)
{
    // Return unique row id.
    (*row_id) = ((struct sqlite3_vtab_cursor_traildb*) cur)->row_idx;
    return SQLITE_OK;
}

static int traildbRename(sqlite3_vtab* cur, const char* newname)
{
    // In this function we can object if sqlite3 wants to rename our table.
    // I don't know any reasons why we would want to stop sqlite3 from doing
    // that so I'm giving it permission every time.
    return SQLITE_OK;
}

static int register_vtab_definitions(sqlite3* db)
{
    // Sqlite3 boilerplate, teach it how to use our table.
    static sqlite3_module traildbModule = {
        1,
        traildbConnect,   // create
        traildbConnect,   // connect
        traildbBestIndex, // bestindex
        traildbDisconnect, // disconnect
        traildbDisconnect, // destroy
        traildbOpenCursor,  // open
        traildbCloseCursor,  // close
        traildbFilter,
        traildbNext,  // next
        traildbEof,  // eof
        traildbColumn,  // column
        traildbRowId,  // row id
        0,  // update
        0,  // begin
        0,  // sync
        0,  // commit
        0,  // rollback
        0,  // findmethod
        traildbRename,  // rename
    };

    int rc = sqlite3_create_module(db, "traildb", &traildbModule, 0);
    return rc;
}

int sqlite3_sqlitetraildb_init(sqlite3* db, char** error_message, const sqlite3_api_routines *api)
{
    SQLITE_EXTENSION_INIT2(api);
    return register_vtab_definitions(db);
}
