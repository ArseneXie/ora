// Copyright 2015 Rana Ian. All rights reserved.
// Use of this source code is governed by The MIT License
// found in the accompanying LICENSE file.

package ora

/*
#include <oci.h>
#include <stdlib.h>
*/
import "C"
import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// SrvCfg configures a new Srv.
type SrvCfg struct {
	// Dblink specifies an Oracle database server. Dblink is a connect string
	// or a service point.
	Dblink string

	// StmtCfg configures new Stmts.
	StmtCfg
}

func (c SrvCfg) IsZero() bool { return c.StmtCfg.IsZero() }

// LogSrvCfg represents Srv logging configuration values.
type LogSrvCfg struct {
	// Close determines whether the Srv.Close method is logged.
	//
	// The default is true.
	Close bool

	// OpenSes determines whether the Srv.OpenSes method is logged.
	//
	// The default is true.
	OpenSes bool

	// Version determines whether the Srv.Version method is logged.
	//
	// The default is true.
	Version bool
}

// NewLogSrvCfg creates a LogSrvCfg with default values.
func NewLogSrvCfg() LogSrvCfg {
	c := LogSrvCfg{}
	c.Close = true
	c.OpenSes = true
	c.Version = true
	return c
}

// Srv represents an Oracle server.
type Srv struct {
	sync.RWMutex

	id     uint64
	cfg    atomic.Value
	env    *Env
	ocisrv *C.OCIServer
	isUTF8 int32

	openSess *sesList

	sysNamer
}

// Cfg returns the Srv's SrvCfg, or it's Env's, if not set.
// If the env is the PkgSqlEnv, that will override StmtCfg!
func (srv *Srv) Cfg() SrvCfg {
	c := srv.cfg.Load()
	var cfg SrvCfg
	if c != nil {
		cfg = c.(SrvCfg)
	}
	env := srv.env
	if cfg.StmtCfg.IsZero() || env.isPkgEnv {
		cfg.StmtCfg = env.Cfg()
	}
	return cfg
}
func (srv *Srv) SetCfg(cfg SrvCfg) {
	srv.cfg.Store(cfg)
}

// Close disconnects from an Oracle server.
//
// Any open sessions associated with the server are closed.
//
// Calling Close will cause Srv.IsOpen to return false. Once closed, a server cannot
// be re-opened. Call Env.OpenSrv to open a new server.
func (srv *Srv) Close() (err error) {
	if srv == nil {
		return nil
	}
	return srv.closeWithRemove()
}
func (srv *Srv) closeWithRemove() error {
	if srv == nil {
		return nil
	}
	srv.RLock()
	env := srv.env
	srv.RUnlock()
	if env == nil {
		return nil
	}
	env.openSrvs.remove(srv)
	return srv.close()
}

// close disconnects from an Oracle server.
// Does not remove Srv from Ses.openSrvs
func (srv *Srv) close() (err error) {
	srv.log(_drv.Cfg().Log.Srv.Close)
	err = srv.checkClosed()
	if err != nil {
		return errE(err)
	}
	srv.RLock()
	openSess := srv.openSess
	srv.RUnlock()
	errs := _drv.listPool.Get().(*list.List)
	defer func() {
		if value := recover(); value != nil {
			errs.PushBack(errR(value))
		}
		srv.SetCfg(SrvCfg{})
		openSess.clear()
		srv.Lock()
		srv.env = nil
		srv.ocisrv = nil
		srv.Unlock()
		_drv.srvPool.Put(srv)

		multiErr := newMultiErrL(errs)
		if multiErr != nil {
			err = errE(*multiErr)
		}
		errs.Init()
		_drv.listPool.Put(errs)
	}()

	openSess.closeAll(errs) // close sessions

	// detach server
	// OCIServerDetach invalidates oci server handle; no need to free server.ocisvr
	// OCIServerDetach invalidates oci service context handle; no need to free server.ocisvcctx
	srv.RLock()
	r := C.OCIServerDetach(
		srv.ocisrv,     //OCIServer   *srvhp,
		srv.env.ocierr, //OCIError    *errhp,
		C.OCI_DEFAULT)  //ub4         mode );
	srv.RUnlock()
	if r == C.OCI_ERROR {
		errs.PushBack(errE(srv.env.ociError()))
	}
	return nil
}

// OpenSes opens an Oracle session returning a *Ses and possible error.
func (srv *Srv) OpenSes(cfg SesCfg) (ses *Ses, err error) {
	if cfg.IsZero() {
		return nil, er("Parameter 'cfg' may not be nil.")
	}
	defer func() {
		if value := recover(); value != nil {
			err = errR(value)
		}
	}()
	if srv == nil {
		return nil, er("srv may not be nil.")
	}
	srv.log(_drv.Cfg().Log.Srv.OpenSes)
	err = srv.checkClosed()
	if err != nil {
		return nil, errE(err)
	}
	// allocate session handle
	ocises, err := srv.env.allocOciHandle(C.OCI_HTYPE_SESSION)
	if err != nil {
		return nil, errE(err)
	}
	credentialType := C.ub4(C.OCI_CRED_EXT)
	if cfg.Username != "" || cfg.Password != "" {
		credentialType = C.OCI_CRED_RDBMS
		// set username on session handle
		cUsername := C.CString(cfg.Username)
		defer C.free(unsafe.Pointer(cUsername))
		err = srv.env.setAttr(ocises, C.OCI_HTYPE_SESSION, unsafe.Pointer(cUsername), C.ub4(len(cfg.Username)), C.OCI_ATTR_USERNAME)
		if err != nil {
			return nil, errE(err)
		}
		// set password on session handle
		cPassword := C.CString(cfg.Password)
		defer C.free(unsafe.Pointer(cPassword))
		err = srv.env.setAttr(ocises, C.OCI_HTYPE_SESSION, unsafe.Pointer(cPassword), C.ub4(len(cfg.Password)), C.OCI_ATTR_PASSWORD)
		if err != nil {
			return nil, errE(err)
		}
	}
	// allocate service context handle
	ocisvcctx, err := srv.env.allocOciHandle(C.OCI_HTYPE_SVCCTX)
	if err != nil {
		return nil, errE(err)
	}
	// set server handle onto service context handle
	err = srv.env.setAttr(ocisvcctx, C.OCI_HTYPE_SVCCTX, unsafe.Pointer(srv.ocisrv), C.ub4(0), C.OCI_ATTR_SERVER)
	if err != nil {
		return nil, errE(err)
	}
	//srv.logF(true, "CRED_EXT? %t username=%q", credentialType == C.OCI_CRED_EXT, username)
	// set driver name on the session handle
	// driver name is specified to aid diagnostics; max 9 single-byte characters
	// driver name will be visible in V$SESSION_CONNECT_INFO or GV$SESSION_CONNECT_INFO as CLIENT_DRIVER
	drvName := fmt.Sprintf("GO%s", Version)
	cDrvName := C.CString(drvName)
	defer C.free(unsafe.Pointer(cDrvName))
	if err = srv.env.setAttr(ocises, C.OCI_HTYPE_SESSION,
		unsafe.Pointer(cDrvName), C.ub4(len(drvName)), C.OCI_ATTR_DRIVER_NAME,
	); err != nil {
		return nil, errE(err)
	}
	// http://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci07lob.htm#CHDDHFAB
	// Set LOB prefetch size to chunk size
	lobPrefetchSize := C.ub4(lobChunkSize)
	if err = srv.env.setAttr(ocises, C.OCI_HTYPE_SESSION,
		unsafe.Pointer(&lobPrefetchSize), C.ub4(0), C.OCI_ATTR_DEFAULT_LOBPREFETCH_SIZE,
	); err != nil {
		return nil, errE(err)
	}

	mode := C.ub4(C.OCI_DEFAULT)
	switch cfg.Mode {
	case SysDba:
		mode |= C.OCI_SYSDBA
	case SysOper:
		mode |= C.OCI_SYSOPER
	}
	// begin session
	srv.RLock()
	r := C.OCISessionBegin(
		(*C.OCISvcCtx)(ocisvcctx), //OCISvcCtx     *svchp,
		srv.env.ocierr,            //OCIError      *errhp,
		(*C.OCISession)(ocises),   //OCISession    *usrhp,
		credentialType,            //ub4           credt,
		mode)                      //ub4           mode );
	srv.RUnlock()
	if r == C.OCI_ERROR {
		return nil, errE(srv.env.ociError())
	}
	// set session handle on service context handle
	err = srv.env.setAttr(unsafe.Pointer(ocisvcctx), C.OCI_HTYPE_SVCCTX, ocises, C.ub4(0), C.OCI_ATTR_SESSION)
	if err != nil {
		return nil, errE(err)
	}
	// set stmt cache size to zero
	// https://docs.oracle.com/database/121/LNOCI/oci09adv.htm#LNOCI16655
	stmtCacheSize := C.ub4(0)
	err = srv.env.setAttr(unsafe.Pointer(ocisvcctx), C.OCI_HTYPE_SVCCTX, unsafe.Pointer(&stmtCacheSize), C.ub4(0), C.OCI_ATTR_STMTCACHESIZE)
	if err != nil {
		return nil, errE(err)
	}

	ses = _drv.sesPool.Get().(*Ses) // set *Ses
	ses.Lock()
	ses.srv = srv
	ses.ocisvcctx = (*C.OCISvcCtx)(ocisvcctx)
	ses.ocises = (*C.OCISession)(ocises)
	if ses.id == 0 {
		ses.id = _drv.sesId.nextId()
	}
	ses.Unlock()
	ses.SetCfg(cfg)
	srv.openSess.add(ses)

	return ses, nil
}

// Version returns the Oracle database server version.
//
// Version requires the server have at least one open session.
func (srv *Srv) Version() (ver string, err error) {
	srv.log(_drv.Cfg().Log.Srv.Version)
	err = srv.checkClosed()
	if err != nil {
		return "", errE(err)
	}
	var buf [512]C.char
	srv.RLock()
	r := C.OCIServerVersion(
		unsafe.Pointer(srv.ocisrv),            //void         *hndlp,
		srv.env.ocierr,                        //OCIError     *errhp,
		(*C.OraText)(unsafe.Pointer(&buf[0])), //OraText      *bufp,
		C.ub4(len(buf)),                       //ub4          bufsz
		C.OCI_HTYPE_SERVER)                    //ub1          hndltype );
	srv.RUnlock()
	if r == C.OCI_ERROR {
		return "", errE(srv.env.ociError())
	}
	return C.GoString(&buf[0]), nil
}

// NumSes returns the number of open Oracle sessions.
func (srv *Srv) NumSes() int {
	if srv == nil {
		return 0
	}
	srv.RLock()
	openSess := srv.openSess
	srv.RUnlock()
	return openSess.len()
}

// IsUTF8 returns whether the DB uses AL32UTF8 encoding.
func (srv *Srv) IsUTF8() bool {
	if srv == nil {
		return false
	}
	return atomic.LoadInt32(&srv.isUTF8) == 1
}

// IsOpen returns true when the server is open; otherwise, false.
//
// Calling Close will cause Srv.IsOpen to return false. Once closed, a server cannot
// be re-opened. Call Env.OpenSrv to open a new server.
func (srv *Srv) IsOpen() bool {
	return srv.checkClosed() == nil
}

// checkClosed returns an error if Srv is closed. No locking occurs.
func (srv *Srv) checkClosed() error {
	if srv == nil {
		return er("Srv is closed.")
	}
	srv.RLock()
	closed := srv.ocisrv == nil
	srv.RUnlock()
	if closed {
		return er("Srv is closed.")
	}
	return srv.env.checkClosed()
}

// sysName returns a string representing the Ses.
func (srv *Srv) sysName() string {
	if srv == nil {
		return "E_S_"
	}
	return srv.sysNamer.Name(func() string { return fmt.Sprintf("%sS%v", srv.env.sysName(), srv.id) })
}

// log writes a message with an Srv system name and caller info.
func (srv *Srv) log(enabled bool, v ...interface{}) {
	if !_drv.Cfg().Log.IsEnabled(enabled) {
		return
	}
	if len(v) == 0 {
		_drv.Cfg().Log.Logger.Infof("%v %v", srv.sysName(), callInfo(1))
	} else {
		_drv.Cfg().Log.Logger.Infof("%v %v %v", srv.sysName(), callInfo(1), fmt.Sprint(v...))
	}
}

// log writes a formatted message with an Srv system name and caller info.
func (srv *Srv) logF(enabled bool, format string, v ...interface{}) {
	if !_drv.Cfg().Log.IsEnabled(enabled) {
		return
	}
	if len(v) == 0 {
		_drv.Cfg().Log.Logger.Infof("%v %v", srv.sysName(), callInfo(1))
	} else {
		_drv.Cfg().Log.Logger.Infof("%v %v %v", srv.sysName(), callInfo(1), fmt.Sprintf(format, v...))
	}
}
