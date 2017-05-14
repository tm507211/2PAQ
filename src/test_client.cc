#include "rpc/client.h"
#include <iostream>
#include <string>
#include <random>
#include <chrono>
#include <vector>
#include <utility>
using namespace std;

#define PRINT_TIME 1000

string random_data(size_t size, mt19937& gen){
  static uniform_int_distribution<> distr(0, 26);
  string tmp;
  tmp.resize('a', size);
  for (size_t i = 0; i < size; ++i){
    tmp[i] += distr(gen);
  }
  return tmp;
}

string get_key(vector<string>& keys, mt19937& gen, bool old = false){
  static uniform_real_distribution<> distr(0,1);
  double prob(0.05);
  if (keys.size() <= 10){
    prob = 1;
  } else if (keys.size() == 100 || old){
    prob = 0;
  }
  if (distr(gen) < prob){ /* generate new key */
    keys.push_back(random_data(100, gen));
    return keys[keys.size()-1];
  } else {
    uniform_int_distribution<> distr_int(0, keys.size());
    return keys[distr_int(gen)];
  }
}

int main(int argc, char ** argv){
  if (argc != 3){
    cerr << "Usage: " << argv[0] << " <load_balancer_ip> <load_balancer_port>";
    return -1;
  }
  //  rpc::client load_balancer(argv[1], stoi(argv[2]));

  pair<string, size_t> serv = make_pair("localhost", 8080); //load_balancer.call("choose_node", "", 0).as<pair<string, size_t>>();

  rpc::client *server = new rpc::client(serv.first, serv.second);
  while (server->get_connection_state() != rpc::client::connection_state::connected);
  
  double put_percent = 0.1; //load_balancer.call("get_put_percent").as<double>();
  double rem_percent = 0; //load_balancer.call("get_rem_percent").as<double>();
  size_t data_size = 500; //load_balancer.call("get_size").as<size_t>();

  random_device rd;
  mt19937 gen(rd());
  uniform_real_distribution<> distr(0, 1);
  double prob;

  vector<string> keys;

  size_t time_put(0), time_rem(0), time_get(0);
  size_t num_put(0), num_rem(0), num_get(0);
  size_t put_min(-1), put_max(0), rem_min(-1), rem_max(0), get_min(-1), get_max(0);
  size_t time_since(0), time_last(0), time, time_units(1);
  while (1){
    auto start_loop = std::chrono::steady_clock::now();
    if (server->get_connection_state() != rpc::client::connection_state::connected){
      delete server;
      //serv = load_balancer.call("choose_node", serv.first, serv.second).as<pair<string, size_t>>();
      server = new rpc::client(serv.first, serv.second);
      while (server->get_connection_state() != rpc::client::connection_state::connected);
    }
    std::cout << "HERE 1" << endl;
    prob = distr(gen);
    if (prob < put_percent){ /* Perform a put operation */
      ++num_put;
      auto start = std::chrono::steady_clock::now();
      //server->call("put", get_key(keys, gen), random_data(data_size, gen));
      auto end = std::chrono::steady_clock::now();
      time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      time_put += time;
      put_min = (time < put_min) ? time : put_min;
      put_max = (time > put_max) ? time : put_max;
    } else if(prob - put_percent < rem_percent) { /* Perform a remove operation */
      if (keys.size() == 0) continue;
      ++num_rem;
      auto start = std::chrono::steady_clock::now();
      //server->call("remove", get_key(keys, gen, true));
      auto end = std::chrono::steady_clock::now();
      time += std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      time_rem += time;
      rem_min = (time < rem_min) ? time : rem_min;
      rem_max = (time > rem_max) ? time : rem_max;
    } else { /* Perform a get operation */
      ++num_get;
      auto start = std::chrono::steady_clock::now();
      server->call("get", get_key(keys, gen)).as<string>();
      auto end = std::chrono::steady_clock::now();
      time += std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      time_get += time;
      get_min = (time < get_min) ? time : get_min;
      get_max = (time > get_max) ? time : get_max;
    }
    std::cout << "HERE 2" << std::endl;
    auto end_loop = std::chrono::steady_clock::now();
    time_since += std::chrono::duration_cast<std::chrono::milliseconds>(end_loop - start_loop).count();
    if (time_since >= time_last + PRINT_TIME){
      cout << "TIME ELAPSED : " << (time_since - time_last) << endl;
      cout << "PUT : " << put_min << " " << ((num_put == 0) ? 0 : 1.0 * time_put / num_put) << " " << put_max << " " << 1.0 * num_put / time_since << endl;
      cout << "REM : " << rem_min << " " << ((num_rem == 0) ? 0 : 1.0 * time_rem / num_rem) << " " << rem_max << " " << 1.0 * num_get / time_since << endl;
      cout << "GET : " << get_min << " " << ((num_get == 0) ? 0 : 1.0 * time_get / num_get) << " " << get_max << " " << 1.0 * num_put / time_since << endl;
      time_put = time_rem = time_get = num_put = num_rem = num_get = 0;
      put_min = rem_min = get_min = -1;
      put_max = rem_max = get_max = 0;
      time_last = time_since;
    }
  }
  delete server;
  return 0;
}
