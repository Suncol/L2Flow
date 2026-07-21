#include <stdio.h>
#include <mdl_api.h>
#include <mdl_shl2_msg.h>
#include <mdl_szl2_msg.h>
#include <mdl_shfel2_msg.h>
#include <mdl_czcel2_msg.h>
#include <mdl_cffexl2_msg.h>
#include <mdl_dcel2_msg.h>
#include <mdl_gfexl2_msg.h>
#include <json/value.h>
#include <json/reader.h>
#include <json/json.h>
#include <set>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <thread>
#include <chrono>
using namespace datayes::mdl;

#if defined(_MSC_VER)
#pragma comment(lib, "mdl_api.lib")
#endif
extern "C" void DTAPIDLLCALL DllGetMDLDateTime(MDLTime* ptm, MDLDate* pdat);

std::string MDLTime2Str(const MDLTime& tm) {
    char buf[15];
    sprintf(buf, "%02d:%02d:%02d.%03d", tm.GetHour(), tm.GetMinute(), tm.GetSecond(), tm.GetMilliSec());
    return std::string(buf);
}
MDLTime CurrentMDLTime() {
    MDLTime time;
    DllGetMDLDateTime(&time, NULL);
    return time;
}

int DiffTime(const MDLTime& lh, const MDLTime& rh) {
    int diff = (int)(lh.GetHour() - rh.GetHour()) * 3600 * 1000
        + (int)(lh.GetMinute() - rh.GetMinute()) * 60 * 1000
        + (int)(lh.GetSecond() - rh.GetSecond()) * 1000
        + (int)(lh.GetMilliSec() - rh.GetMilliSec());
    return diff;
}


/*Config.json
[
{"Address":"mdl01", "token":"User1", "Service":[{"SID":4, "MID":18, "SecurityIDs": ["600000","688006"]}, {"SID":4, "MID":19, "SecurityIDs": ["600000","688006"]}] },
{"Address":"mdl02", "token":"User2", "Service":[{"SID":6, "MID":33, "SecurityIDs": ["000001","300115"]}, {"SID":6, "MID":36, "SecurityIDs": ["000001","300115"]}] }
]
*/
struct Config {
    typedef std::pair<int, int> MSGID;
    struct UpStreamServer {
        std::string addr;
        std::string token;
        std::vector<MSGID> msgs;
    };
    bool LoadFromFile(const char* fileName) {
        Json::Reader reader;
        Json::Value root;
        std::ifstream file;
        file.open(fileName, std::ios::in | std::ios::binary);
        if (!file.good()) {
            printf("Load Config Failed:Open File Failed\n");
            return false;
        }
        if (!reader.parse(file, root)) {
            printf("Load Config Failed:%s!\n", reader.getFormatedErrorMessages().c_str());
            return false;
        }

        if (root.isNull() || !root.isArray()) {
            printf("Load Config Failed: root isn't an Array() \n");
            return false;
        }

        for (int i = 0; i < root.size(); ++i) {
            Json::Value& upcfg = root[i];
            UpStreamServer upstream;
            upstream.addr = upcfg["Address"].asString();
            upstream.token = upcfg["token"].asString();
            if (upcfg["Service"].isNull() || !upcfg["Service"].isArray()) {
                continue;
            }
            for (int j = 0; j < upcfg["Service"].size(); ++j) {
                auto& msgcfg = upcfg["Service"][j];
                int sid = msgcfg["SID"].asInt();
                int mid = msgcfg["MID"].asInt();
                std::set<std::string> secs;
                if (msgcfg["SecurityIDs"].isNull() || !msgcfg["SecurityIDs"].isArray()) {
                    continue;
                }
                for (int k = 0; k < msgcfg["SecurityIDs"].size(); ++k) {
                    std::string secid = msgcfg["SecurityIDs"][k].asString();
                    if (!secid.empty()) {
                        secs.insert(secid);
                    }
                }
                if (!secs.empty()) {
                    upstream.msgs.push_back({ sid, mid });
                    output_securities.insert({ { sid, mid }, secs });
                }
            }
            if (!upstream.msgs.empty()) {
                servers.push_back(upstream);
            }
        }
        if (servers.empty()) {
            printf("Load Config Failed: Empty UpStreams!\n");
            return false;
        }
        return true;
    }

    bool Filter(int sid, int mid, const std::string & secid) {
        auto iter = output_securities.find({sid, mid});
        if (iter == output_securities.end()) {
            return false;
        }
        return iter->second.find(secid) != iter->second.end();
    }
    std::vector<UpStreamServer> servers;
    std::map<MSGID, std::set<std::string>> output_securities;
};

class MyMessageHandler : public MessageHandler {
 public:
     MyMessageHandler(Config& c): cfg(c){
         char file_name[40];
         auto tm = CurrentMDLTime();
         sprintf(file_name, "latency_output_%02d%02d%02d.txt", tm.GetHour(), tm.GetMinute(), tm.GetSecond());
         file.open(file_name, std::ios::out);
         if (!file.is_open()) {
             printf("Cannot open file:%s\n", file_name);
             return;
         }

         file << "ServiceID,MessageID,UpdateTime,TL_LocalTime,ReceiveTime,Latency,ForwardLatncy\n";
     }
     ~MyMessageHandler() {
         if (!file.is_open()) {
             file.close();
         }
     }
     // handle network failure
    virtual void OnMDLAPIMessage(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_api_msg::ConnectingEvent::MessageID) {
            mdl_api_msg::ConnectingEvent* resp = (mdl_api_msg::ConnectingEvent*)msg->GetBody();
            printf("Connect to %s ...\n", resp->Address.c_str());
        }
        else if (head->MessageID == mdl_api_msg::ConnectErrorEvent::MessageID) {
            mdl_api_msg::ConnectErrorEvent* resp = (mdl_api_msg::ConnectErrorEvent*)msg->GetBody();
            printf("Connect to %s failed %s.\n", resp->Address.c_str(), resp->ErrorMessage.c_str());
        }
        else if (head->MessageID == mdl_api_msg::DisconnectedEvent::MessageID) {
            mdl_api_msg::DisconnectedEvent* resp = (mdl_api_msg::DisconnectedEvent*)msg->GetBody();
            printf("Disconnected from %s: %s.\n", resp->Address.c_str(), resp->ErrorMessage.c_str());
        }
    }
    void OnMDLSysMessage(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_sys_msg::LogonResponse::MessageID) {
            mdl_sys_msg::LogonResponse* resp = (mdl_sys_msg::LogonResponse*)msg->GetBody();
            printf("LogonResponse: ReturnCode(%d)", resp->ReturnCode);
            for (uint32_t i = 0; i < resp->Services.Length; ++i) {
                for (uint32_t j = 0; j < resp->Services[i]->Messages.Length; ++j) {
                    printf(" %d.%d.%d(subscribe %s)", 
                        resp->Services[i]->ServiceID, resp->Services[i]->ServiceVersion, resp->Services[i]->Messages[j]->MessageID,
                        resp->Services[i]->Messages[j]->MessageStatus == MDLEC_OK ? "ok" : "fail");
                }
            }
            printf("\n");
        }
    }

    template<class T>
    void Write2File(const MDLMessageHead* head, const std::string SecurityID, const MDLTime& updatetime) {
        if (!file.is_open()) {
            return;
        }
        if (cfg.Filter(head->ServiceID, head->MessageID, SecurityID)) {
            auto now = CurrentMDLTime();
            file << (int)head->ServiceID << "," << (int)head->MessageID << "," << SecurityID << "," << MDLTime2Str(updatetime) << ","
                << MDLTime2Str(head->LocalTime) << "," << MDLTime2Str(now) << ","
                << DiffTime(now, updatetime) << "," << DiffTime(now, head->LocalTime) <<"\n";
        }
    }
#define W2F(head, body, SecurityID, updatetime) Write2File<std::decay<decltype(*body)>::type>(head, body->SecurityID.std_str(), body->updatetime)
    virtual void OnMDLSHL2Message(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_shl2_msg::SHL2Transaction2::MessageID) {
            mdl_shl2_msg::SHL2Transaction2* body = (mdl_shl2_msg::SHL2Transaction2*)msg->GetBody();
            W2F(head, body, SecurityID, TradTime);
        }
        else if (head->MessageID == mdl_shl2_msg::Order::MessageID) {
            mdl_shl2_msg::Order* body = (mdl_shl2_msg::Order*)msg->GetBody();
            W2F(head, body, SecurityID, OrderTime);
        }
    }
    virtual void OnMDLSZL2Message(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_szl2_msg::Order300192_v2::MessageID) {
            mdl_szl2_msg::Order300192_v2* body = (mdl_szl2_msg::Order300192_v2*)msg->GetBody();
            W2F(head, body, SecurityID, TransactTime);
        }
        else if (head->MessageID == mdl_szl2_msg::Transaction300191_v2::MessageID) {
            mdl_szl2_msg::Transaction300191_v2* body = (mdl_szl2_msg::Transaction300191_v2*)msg->GetBody();
            W2F(head, body, SecurityID, TransactTime);
        }
    }
   virtual void OnMDLSHFEL2Message(const MDLMessage* msg) {
       MDLMessageHead* head = msg->GetHead();
       if (head->MessageID == mdl_shfel2_msg::CTPFuture::MessageID) {
           mdl_shfel2_msg::CTPFuture* body = (mdl_shfel2_msg::CTPFuture*)msg->GetBody();
           W2F(head, body, InstruID, UpdateTime);
       }
       else if (head->MessageID == mdl_shfel2_msg::CrudeFuture::MessageID) {
           mdl_shfel2_msg::CrudeFuture* body = (mdl_shfel2_msg::CrudeFuture*)msg->GetBody();
           W2F(head, body, InstruID, UpdateTime);
       }
   }
   virtual void OnMDLCZCEL2Message(const MDLMessage* msg) {
       MDLMessageHead* head = msg->GetHead();
       if (head->MessageID == mdl_czcel2_msg::CTPFuture::MessageID) {
           mdl_czcel2_msg::CTPFuture* body = (mdl_czcel2_msg::CTPFuture*)msg->GetBody();
           W2F(head, body, InstruID, UpdateTime);
       }
   }
   virtual void OnMDLDCEL2Message(const MDLMessage* msg)  {
       MDLMessageHead* head = msg->GetHead();
       if (head->MessageID == mdl_dcel2_msg::Future::MessageID) {
           mdl_dcel2_msg::Future* body = (mdl_dcel2_msg::Future*)msg->GetBody();
           W2F(head, body, InstruID, UpdateTime);
       }
   }
   virtual void OnMDLCFFEXL2Message(const MDLMessage* msg){
       MDLMessageHead* head = msg->GetHead();
       if (head->MessageID == mdl_cffexl2_msg::Future::MessageID) {
           mdl_cffexl2_msg::Future* body = (mdl_cffexl2_msg::Future*)msg->GetBody();
           W2F(head, body, InstruID, UpdateTime);
       }
   }
   virtual void OnMDLGFEXL2Message(const MDLMessage* msg) {
       MDLMessageHead* head = msg->GetHead();
   }

   Config& cfg;
   std::ofstream file;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[]) { 

    Config cfg;
    if (!cfg.LoadFromFile("Config.json")) {
        return 1;
    }

    IOManagerPtr m_IOManager = CreateIOManager(1);
    if (m_IOManager.IsNull()) {
        printf("Incompatible API lib version.\n");
        return 2;
    }
    
    MyMessageHandler handler(cfg);
    for (auto & svr : cfg.servers) {
        SubscriberPtr sub = m_IOManager->CreateSubscriber(&handler);
        sub->SetServerAddress(svr.addr.c_str());
        sub->SetUserName(svr.token.c_str());
        sub->SetMessageEncoding(MDLEID_MKTPRO);

        for (auto & s : svr.msgs) {
            sub->AddSubscription(s.first, 101, s.second);
        }

        // connect to server
        std::string err = sub->Connect();
        if (err.empty()) {
            printf("Connect to server successfully.\n");
            continue;
        }
        printf("Connect to server failed: %s.\n", err.c_str());
    }

    std::this_thread::sleep_for(std::chrono::seconds(3));
    printf("Press enter to exit.\n");
    getchar();
    m_IOManager->Shutdown();
    m_IOManager.Reset();

    return 0;
}

