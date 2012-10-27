#include <iostream>
#include "ibis.h"

using namespace std;
void dump(ibis::table * table_select, ibis::util::logger& lg) {
  table_select->dump(lg(), ",");
}
int main() {
  ibis::gVerbose = 1;
  ibis::util::logger lg;
  string s("/Users/gaurav/Downloads/ecpm");
  ibis::table * table = ibis::table::create(s.c_str());
  ibis::table * table_select = table->select("pub,sum(req)","pub='123GPRS Team'");
//  table_select->dump(lg(), ",");
//  dump(table_select, lg);

//  lg() << "now doing cursor dump" << endl;
  ibis::table::cursor* cursor = table_select->createCursor();
  cursor->fetch();
  string val;
  cursor->getColumnAsString(0U, val);
  double m;
  cursor->getColumnAsDouble(1U, m);
  lg() << "colAsString:" << val << "--" << m;
  delete table;
}


