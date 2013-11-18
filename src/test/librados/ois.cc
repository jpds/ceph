#include <iostream>
#include <errno.h>

#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "gtest/gtest.h"

using namespace librados;

TEST(OIS, ReturnConst) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ASSERT_EQ(0, ioctx.create("obj", false));

  ObjectReadOperation op;
  op.ois_ret(222);

  bufferlist bl;
  ASSERT_EQ(222, ioctx.operate("obj", &op, &bl));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

static void build_op(ObjectReadOperation& op, bufferlist& bl)
{
  // puts -ENOENT in "ret" register
  op.getxattr("foo", &bl, NULL);
  // jump to has_attr if attribute exists
  op.ois_jge("ret", 0, "has_attr");
  // jump to no_attr if attribute doesn't exist
  op.ois_jeq("ret", -ENODATA, "no_attr");
  // return the error that getxattr generated
  op.ois_ret("ret");
  // return 777 if the attr exists
  op.ois_label("has_attr");
  op.ois_ret(777);
  // retrun 555 if the attr doesn't exist
  op.ois_label("no_attr");
  op.ois_ret(555);
}

TEST(OIS, Branch) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  std::string rnd_obj = "obj";

  ASSERT_EQ(0, ioctx.create(rnd_obj, false));

  bufferlist bl;

  ObjectReadOperation op;
  
  // takes no_attr branch
  build_op(op, bl);
  ASSERT_EQ(555, ioctx.operate(rnd_obj, &op, &bl));

  // new branch will be taken
  bl.append(pool_name.c_str(), pool_name.length());
  ASSERT_EQ(0, ioctx.setxattr(rnd_obj, "foo", bl));

  // takes has_attr branch
  ObjectReadOperation op2;
  build_op(op2, bl);
  ASSERT_EQ(777, ioctx.operate(rnd_obj, &op2, &bl));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
