// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "PGBackend.h"

// -- ObjectModDesc --
struct RollbackVisitor : public ObjectModDesc::Visitor {
  const hobject_t &hoid;
  PGBackend *pg;
  ObjectStore::Transaction *t;
  RollbackVisitor(
    const hobject_t &hoid,
    PGBackend *pg,
    ObjectStore::Transaction *t) : hoid(hoid), pg(pg), t(t) {}
  void append(uint64_t old_size) {
    pg->rollback_append(hoid, old_size, t);
  }
  void setattrs(map<string, boost::optional<bufferlist> > &attrs) {
    pg->rollback_setattrs(hoid, attrs, t);
  }
  void rmobject(version_t old_version) {
    pg->rollback_unstash(hoid, old_version, t);
  }
  void create() {
    pg->rollback_create(hoid, t);
  }
};

void PGBackend::rollback(
  const hobject_t &hoid,
  ObjectModDesc &desc,
  ObjectStore::Transaction *t)
{
  assert(desc.can_rollback());
  RollbackVisitor vis(hoid, this, t);
  desc.visit(&vis);
}


