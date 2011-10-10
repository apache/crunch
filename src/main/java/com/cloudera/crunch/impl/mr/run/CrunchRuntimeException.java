package com.cloudera.crunch.impl.mr.run;

public class CrunchRuntimeException extends RuntimeException {

  private boolean logged = false;
  
  public CrunchRuntimeException(String msg) {
    super(msg);
  }
  
  public CrunchRuntimeException(Exception e) {
    super(e);
  }
  
  public boolean wasLogged() {
    return logged;
  }
  
  public void markLogged() {
    this.logged = true;
  }
}
