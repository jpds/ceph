// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "HitSet.h"

// -- HitSet --

HitSet::HitSet(HitSet::Params *params)
{
  switch(params->get_type()) {
  case TYPE_BLOOM: {
    BloomHitSet::Params *p = static_cast<BloomHitSet::Params*>(params);
    impl.reset(new BloomHitSet(p));
  }
    break;
  case TYPE_EXPLICIT_HASH: {
    ExplicitHashHitSet::Params *p = static_cast<ExplicitHashHitSet::Params*>(
	params);
    impl.reset(new ExplicitHashHitSet(p));
  }
    break;
  case TYPE_EXPLICIT_OBJECT: {
    ExplicitObjectHitSet::Params *p = static_cast<ExplicitObjectHitSet::Params*>(
	params);
    impl.reset(new ExplicitObjectHitSet(p));
  }
    break;
  case TYPE_NONE:
    break;
  default:
    assert (0 == "unknown HitSet type");
  }
}

void HitSet::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  if (impl) {
    ::encode((__u8)impl->get_type(), bl);
    impl->encode(bl);
  } else {
    ::encode((__u8)TYPE_NONE, bl);
  }
  ENCODE_FINISH(bl);
}

void HitSet::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  __u8 type;
  ::decode(type, bl);
  switch ((impl_type_t)type) {
  case TYPE_EXPLICIT_HASH:
    impl.reset(new ExplicitHashHitSet);
    break;
  case TYPE_EXPLICIT_OBJECT:
    impl.reset(new ExplicitObjectHitSet);
    break;
  case TYPE_BLOOM:
    impl.reset(new BloomHitSet);
    break;
  case TYPE_NONE:
    impl.reset(NULL);
    break;
  default:
    throw buffer::malformed_input("unrecognized HitMap type");
  }
  if (impl)
    impl->decode(bl);
  DECODE_FINISH(bl);
}

void HitSet::dump(Formatter *f) const
{
  f->dump_string("type", get_type_name());
  if (impl)
    impl->dump(f);
}

void HitSet::generate_test_instances(list<HitSet*>& o)
{
  o.push_back(new HitSet);
  o.push_back(new HitSet(new BloomHitSet(10, .1, 1)));
  o.back()->insert(hobject_t());
  o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
  o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  o.push_back(new HitSet(new ExplicitHashHitSet));
  o.back()->insert(hobject_t());
  o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
  o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  o.push_back(new HitSet(new ExplicitObjectHitSet));
  o.back()->insert(hobject_t());
  o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
  o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
}

HitSet::Params *HitSet::Params::create_copy(const Params *p)
{
  Params *params = NULL;
  switch (p->type) {
  case TYPE_EXPLICIT_HASH:
    params = new ExplicitHashHitSet::Params(*p->get_as_type<ExplicitHashHitSet>());
    break;
  case TYPE_EXPLICIT_OBJECT:
    params = new ExplicitObjectHitSet::Params(*p->get_as_type<ExplicitObjectHitSet>());
    break;
  case TYPE_BLOOM:
    params = new BloomHitSet::Params(*p->get_as_type<BloomHitSet>());
    break;
  case TYPE_NONE:
    params = new HitSet::Params;
    break;
  default: throw buffer::malformed_input("unrecognized HitSet::Params type");
  }
  return params;
}

void HitSet::Params::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode((__u8)type, bl);
  _encode_impl_bits(bl);
  ENCODE_FINISH(bl);
}

// this must match Params::encode bufferlist ordering!
void HitSet::Params::Decoder::decode(bufferlist::iterator &bl)
{
  delete params;
  DECODE_START(1, bl);
  __u8 t;
  ::decode(t, bl);
  impl_type_t type = static_cast<impl_type_t>(t);
  switch (type) {
  case TYPE_EXPLICIT_HASH:
    params = new ExplicitHashHitSet::Params;
    break;
  case TYPE_EXPLICIT_OBJECT:
    params = new ExplicitObjectHitSet::Params;
    break;
  case TYPE_BLOOM:
    params = new BloomHitSet::Params;
    break;
  case TYPE_NONE:
    params = new HitSet::Params;
    break;
  default: throw buffer::malformed_input("unrecognized HitSet::Params type");
  }
  params->_decode_impl_bits(bl);
  DECODE_FINISH(bl);
}

void HitSet::Params::dump(Formatter *f) const
{
  f->dump_string("type", HitSet::get_type_name(type));
  f->open_object_section("impl_params");
  _dump_impl(f);
  f->close_section();
}

void HitSet::Params::generate_test_instances(list<HitSet::Params*>& o)
{
  o.push_back(new Params);

  o.push_back(new Params(TYPE_EXPLICIT_HASH));
  o.push_back(new Params(TYPE_EXPLICIT_OBJECT));
  o.push_back(new Params(TYPE_BLOOM));

#define loop_hitset_params(kind) \
{ \
  list<kind::Params*> params; \
  kind::Params::generate_test_instances(params); \
  for (list<kind::Params*>::iterator i = params.begin(); \
  i != params.end(); ++i) \
    o.push_back(*i); \
}
  loop_hitset_params(BloomHitSet);
  loop_hitset_params(ExplicitObjectHitSet);
  loop_hitset_params(ExplicitHashHitSet);
}

void HitSet::Params::Decoder::generate_test_instances(list<Decoder *>& o)
{
  list<HitSet::Params *> params;
  HitSet::Params::generate_test_instances(params);
  for (list<HitSet::Params*>::iterator i = params.begin();
      i != params.end();
      ++i)
    o.push_back(new Decoder(*i));
}

ostream& operator<<(ostream& out, const HitSet::Params& p) {
  out << "params type:" << HitSet::get_type_name(p.get_type())
      << " impl params {";
  p._dump_impl_stream(out);
  out << "}";
  return out;
}
