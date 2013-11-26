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

#ifndef CEPH_OSD_HITSET_H
#define CEPH_OSD_HITSET_H

#include <boost/scoped_ptr.hpp>

#include "include/encoding.h"
#include "common/bloom_filter.hpp"
#include "common/hobject.h"
#include "common/Formatter.h"

/**
 * generic container for a HitSet
 *
 * Encapsulate a HitSetImpl of any type.  Expose a generic interface
 * to users and wrap the encoded object with a type so that it can be
 * safely decoded later.
 */

class HitSet {
public:
  typedef enum {
    TYPE_NONE = 0,
    TYPE_EXPLICIT_HASH = 1,
    TYPE_EXPLICIT_OBJECT = 2,
    TYPE_BLOOM = 3
  } impl_type_t;

  static const char *get_type_name(impl_type_t t) {
    switch (t) {
    case TYPE_NONE: return "none";
    case TYPE_EXPLICIT_HASH: return "explicit_hash";
    case TYPE_EXPLICIT_OBJECT: return "explicit_object";
    case TYPE_BLOOM: return "bloom";
    default: return "???";
    }
  }
  const char *get_type_name() const {
    if (impl)
      return get_type_name(impl->get_type());
    return get_type_name(TYPE_NONE);
  }

  /// abstract interface for a HitSet implementation
  class Impl {
  public:
    virtual impl_type_t get_type() const = 0;
    virtual void insert(const hobject_t& o) = 0;
    virtual bool contains(const hobject_t& o) const = 0;
    virtual unsigned insert_count() const = 0;
    virtual unsigned approx_unique_insert_count() const = 0;
    virtual void encode(bufferlist &bl) const = 0;
    virtual void decode(bufferlist::iterator& p) = 0;
    virtual void dump(Formatter *f) const = 0;
    /// optimize structure for a desired false positive probability
    virtual void optimize() {}
    virtual ~Impl() {}
  };

  boost::scoped_ptr<Impl> impl;

  class Params {
    impl_type_t type; ///< Type of HitSet
    static void generate_test_instances(list<HitSet::Params*>& o);
  protected:
    Params(impl_type_t t) : type(t) {}
    Params() : type(TYPE_NONE) {}
    /**
     * implementations must specify each of these functions, and include
     * static const type_code member
     */

    /// encode subtype-specific data. Default is empty but versioned
    virtual void _encode_impl_bits(bufferlist &bl) const {
      ENCODE_START(1, 1, bl);
      ENCODE_FINISH(bl);
    }
    /// decode subtype-specific data. Default is empty but versioned
    virtual void _decode_impl_bits(bufferlist::iterator &bl) {
      DECODE_START(1, bl);
      DECODE_FINISH(bl);
    }
    /// dump the subtype-specific data to an ostream
    virtual void _dump_impl_stream(ostream& o) const {}
    /// dump the subtype-specific data to a formatter
    virtual void _dump_impl(Formatter *f) const {}
  public:
    static const impl_type_t type_code = TYPE_NONE;
    virtual ~Params() {}
    impl_type_t get_type() const { return type; }
    void dump(Formatter *f) const;
    void encode(bufferlist &bl) const;

    Params(const Params& o); // not implemented
    Params& operator=(const Params& o); // not implemented
    static Params *create_copy(const Params *p);

    // no encode/decode tests on this type; use ParamsEncoder for that

    /**
     * Get a pointer of the proper subtype for this Params.
     * @return pointer of proper type, or NULL if the Params object is not of
     * the requested type.
     */
    template <typename T> typename T::Params* get_as_type() {
      if (T::Params::type_code == get_type()) {
	return static_cast< typename T::Params* >(this);
      } else {
	return NULL;
      }
    }

    template <typename T> const typename T::Params* get_as_type() const {
      if (T::Params::type_code == get_type()) {
	return static_cast< const typename T::Params* >(this);
      } else {
	return NULL;
      }
    }

    /**
     * Use the Decoder to decode (and optionally encode) Params. By default
     * it will delete the decode params when the Decoder is destroyed, but
     * if you call extract_params() it will remove its reference entirely
     * and the caller takes responsibility.
     */
    class Decoder {
    private:
      Params *params;
    public:
      Decoder() : params(NULL) {}
      Decoder(Params *p) : params(p) {}
      ~Decoder() { delete params; }
      void dump(Formatter *f) const { if (params) params->dump(f); }
      void encode(bufferlist &bl) const {
	Params *p = params;
	if (!p)
	  p = new HitSet::Params;
	p->encode(bl);
      }
      void decode(bufferlist::iterator &bl);
      static void generate_test_instances(list<Decoder *>& o);
      impl_type_t get_type() const {
	return (params ? params->type : TYPE_NONE);
      }
      /// get a read-only pointer to the params
      const Params *get_params() const { return params; }
      /// get a pointer to the params and remove it from Decoder's tracking
      Params *extract_params() { return params; params = NULL; }
      void reset_params(Params *p) { delete params; params = p; }
    };
    friend class Decoder;
    friend ostream& operator<<(ostream& out, const HitSet::Params& p);
  };

  HitSet() : impl(NULL) {}
  HitSet(Impl *i) : impl(i) {}
  HitSet(HitSet::Params *params);

  HitSet(const HitSet& o) {
    // only allow copying empty instances... FIXME
    assert(!o.impl);
  }

  /// insert a hash into the set
  void insert(const hobject_t& o) {
    impl->insert(o);
  }

  /// query whether a hash is in the set
  bool contains(const hobject_t& o) const {
    return impl->contains(o);
  }

  unsigned insert_count() const {
    return impl->insert_count();
  }
  unsigned approx_unique_insert_count() const {
    return impl->approx_unique_insert_count();
  }
  void optimize() {
    impl->optimize();
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<HitSet*>& o);

private:
  void reset_to_type(impl_type_t type);
};
WRITE_CLASS_ENCODER(HitSet);
inline void encode(const HitSet::Params &p, bufferlist &bl, uint64_t features=0) {
  ENCODE_DUMP_PRE(); p.encode(bl); ENCODE_DUMP_POST(p);
}
WRITE_CLASS_ENCODER(HitSet::Params::Decoder);

ostream& operator<<(ostream& out, const HitSet::Params& p);

/**
 * explicitly enumerate hash hits in the set
 */
class ExplicitHashHitSet : public HitSet::Impl {
  uint64_t count;
  hash_set<uint32_t> hits;
public:
  class Params : public HitSet::Params {
  public:
    static const HitSet::impl_type_t type_code = HitSet::TYPE_EXPLICIT_HASH;
    Params() : HitSet::Params(type_code) {}
    Params(const Params &o) : HitSet::Params(type_code) {}
    ~Params() {}
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
    }
  };

  ExplicitHashHitSet() : count(0) {}
  ExplicitHashHitSet(const ExplicitHashHitSet::Params *p) : count(0) {}

  HitSet::impl_type_t get_type() const {
    return HitSet::TYPE_EXPLICIT_HASH;
  }
  void insert(const hobject_t& o) {
    hits.insert(o.hash);
    ++count;
  }
  bool contains(const hobject_t& o) const {
    return hits.count(o.hash);
  }
  unsigned insert_count() const {
    return count;
  }
  unsigned approx_unique_insert_count() const {
    return hits.size();
  }
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(count, bl);
    ::encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(count, bl);
    ::decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_unsigned("insert_count", count);
    f->open_array_section("hash_set");
    for (hash_set<uint32_t>::const_iterator p = hits.begin(); p != hits.end(); ++p)
      f->dump_unsigned("hash", *p);
    f->close_section();
  }
  static void generate_test_instances(list<ExplicitHashHitSet*>& o) {
    o.push_back(new ExplicitHashHitSet);
    o.push_back(new ExplicitHashHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(ExplicitHashHitSet)

/**
 * explicitly enumerate objects in the set
 */
class ExplicitObjectHitSet : public HitSet::Impl {
  uint64_t count;
  hash_set<hobject_t> hits;
public:
  class Params : public HitSet::Params {
  public:
    static const HitSet::impl_type_t type_code = HitSet::TYPE_EXPLICIT_OBJECT;
    Params() : HitSet::Params(type_code) {}
    Params(const Params &o) : HitSet::Params(type_code) {}
    ~Params() {}

    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
    }
  };

  ExplicitObjectHitSet() : count(0) {}
  ExplicitObjectHitSet(const ExplicitObjectHitSet::Params *p) : count(0) {}

  HitSet::impl_type_t get_type() const {
    return HitSet::TYPE_EXPLICIT_OBJECT;
  }
  void insert(const hobject_t& o) {
    hits.insert(o);
    ++count;
  }
  bool contains(const hobject_t& o) const {
    return hits.count(o);
  }
  unsigned insert_count() const {
    return count;
  }
  unsigned approx_unique_insert_count() const {
    return hits.size();
  }
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(count, bl);
    ::encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(count, bl);
    ::decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_unsigned("insert_count", count);
    f->open_array_section("set");
    for (hash_set<hobject_t>::const_iterator p = hits.begin(); p != hits.end(); ++p) {
      f->open_object_section("object");
      p->dump(f);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<ExplicitObjectHitSet*>& o) {
    o.push_back(new ExplicitObjectHitSet);
    o.push_back(new ExplicitObjectHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(ExplicitObjectHitSet)

/**
 * use a bloom_filter to track hits to the set
 */
class BloomHitSet : public HitSet::Impl {
  compressible_bloom_filter bloom;

public:
  HitSet::impl_type_t get_type() const {
    return HitSet::TYPE_BLOOM;
  }

  class Params : public HitSet::Params {
  public:
    static const HitSet::impl_type_t type_code = HitSet::TYPE_BLOOM;

    double false_positive; ///< false positive probability
    uint64_t target_size; ///< number of unique insertions we expect to this HitSet
    uint64_t seed; ///< seed to use when initializing the bloom filter

    Params() : false_positive(0), target_size(0), seed(0) {}
    Params(double fpp, uint64_t t, uint64_t s) :
      HitSet::Params(HitSet::TYPE_BLOOM),
      false_positive(fpp), target_size(t), seed(s) {}
    Params(const Params &o) : HitSet::Params(type_code),
	false_positive(o.false_positive),
	target_size(o.target_size), seed(o.seed) {}
    ~Params() {}
    void _encode_impl_bits(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      uint16_t fpp_micro = static_cast<uint16_t>(false_positive * 1000000.0);
      ::encode(fpp_micro, bl);
      ::encode(target_size, bl);
      ::encode(seed, bl);
      ENCODE_FINISH(bl);
    }
    void _decode_impl_bits(bufferlist::iterator& bl) {
      DECODE_START(1, bl);
      uint16_t fpp_micro;
      ::decode(fpp_micro, bl);
      false_positive = fpp_micro * 1000000.0;
      ::decode(target_size, bl);
      ::decode(seed, bl);
      DECODE_FINISH(bl);
    }
    void _dump_impl(Formatter *f) const {
      f->dump_int("false_positive_probability", false_positive);
      f->dump_int("target_size", target_size);
      f->dump_int("seed", seed);
    }
    void _dump_impl_stream(ostream& o) const {
      o << "false_positive_probability: "
	<< false_positive << ", target size: " << target_size
	<< ", seed: " << seed;
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
      o.push_back(new Params);
      (*o.rbegin())->false_positive = 10;
      (*o.rbegin())->target_size = 300;
      (*o.rbegin())->seed = 99;
    }
  };

  BloomHitSet() {}
  BloomHitSet(unsigned inserts, double fpp, int seed)
    : bloom(inserts, fpp, seed)
  {}
  BloomHitSet(const BloomHitSet::Params *p) : bloom(p->target_size,
                                                    p->false_positive,
                                                    p->seed)
  {}

  void insert(const hobject_t& o) {
    bloom.insert(o.hash);
  }
  bool contains(const hobject_t& o) const {
    return bloom.contains(o.hash);
  }
  unsigned insert_count() const {
    return bloom.element_count();
  }
  unsigned approx_unique_insert_count() const {
    return bloom.approx_unique_element_count();
  }
  void optimize() {
    // aim for a density of .5 (50% of bit set)
    double pc = (double)bloom.density() * 2.0 * 100.0;
    if (pc < 100.0)
      bloom.compress(pc);
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(bloom, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(bloom, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->open_object_section("bloom_filter");
    bloom.dump(f);
    f->close_section();
  }
  static void generate_test_instances(list<BloomHitSet*>& o) {
    o.push_back(new BloomHitSet);
    o.push_back(new BloomHitSet(10, .1, 1));
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(BloomHitSet)

#endif
