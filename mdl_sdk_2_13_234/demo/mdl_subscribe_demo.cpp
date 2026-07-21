#include "tools.h"
#include "mdl_api.h"
#include "mdl_shl1_msg.h"
#include "mdl_szl1_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"
#include "mdl_sge_msg.h"
#include <cstdio>
#include <thread>
#include <chrono>

using namespace datayes::mdl;

class MyMessageHandler : public MessageHandler {
 public:
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
        else if (head->MessageID == mdl_sys_msg::ServiceStatus::MessageID) {
            mdl_sys_msg::ServiceStatus* resp = (mdl_sys_msg::ServiceStatus*)msg->GetBody();
            printf("status: version[%d]client[%d]\n"
                "    start time[%d-%d]send[%.2fkb/s]\n"
                "    memory rss[%.2fmb]peak[%.2fmb]delay[%.2fkb]\n", 
                resp->Version,
                resp->ClientCount, resp->StartDate.m_Value, resp->StartTime.m_Value,
                (double)resp->SendRate / (double)1024,
                (double)resp->MemoryRSS / (double)(1024 * 1024), 
                (double)resp->MemoryPeak / (double)(1024 * 1024),
                (double)resp->BytesDelayed / (double)(1024));
            for (uint32_t i = 0; i < resp->Services.Length; ++i) {
                printf("    service[%d]recv[%.2fkb/s]idle[%d]\n",
                    resp->Services[i]->ServiceID, 
                    (double)resp->Services[i]->ReceiveRate / (double)1024, 
                    resp->Services[i]->IdleTime);
            }
        }
        else if (head->MessageID == mdl_sys_msg::SessionStatus::MessageID) {
            mdl_sys_msg::SessionStatus* resp = (mdl_sys_msg::SessionStatus*)msg->GetBody();
            printf("SessionStatus: client[%d]\n", resp->Clients.Length);
            for (uint32_t i = 0; i < resp->Clients.Length; ++i) {
                printf("    [%s]ver[%d]codec[%d,%d]sub[%s]delay[%.2fkb]\n",
                    resp->Clients[i]->Address.c_str(), 
                    resp->Clients[i]->Version,
                    resp->Clients[i]->EncodeType,
                    resp->Clients[i]->DecodeType,
                    resp->Clients[i]->SubscriptionList.c_str(),
                    (double)resp->Clients[i]->BytesDelayed / (double)(1024));
            }
        }
        else if (head->MessageID == mdl_sys_msg::FeederStatus::MessageID) {
            mdl_sys_msg::FeederStatus* resp = (mdl_sys_msg::FeederStatus*)msg->GetBody();
            printf("[%d]%04d-%02d-%02d, fc %d, fs %ld, df %ld, ec %d, wc %d, us %d, mc %d [%s]\n",
                resp->ServiceNum,
                resp->StartDate.GetYear(),resp->StartDate.GetMonth(),resp->StartDate.GetDay(),
                resp->FileCount, resp->FileSize, resp->DiskFree, resp->ErrorCount, resp->WarnCount, resp->UpStreamStatus, resp->Messages.Length,
                resp->Notes.c_str());
            size_t sum = 0;
            for (size_t i = 0; i < resp->Messages.Length; ++i) {
                printf("\tmsg %d, c %ld, %d:%d:%d\n", 
                    resp->Messages[i]->MessageID, 
                    resp->Messages[i]->MessageCount,
                    resp->Messages[i]->LastTime.GetHour(),
                    resp->Messages[i]->LastTime.GetMinute(),
                    resp->Messages[i]->LastTime.GetSecond());
            }
        }
        else if (head->MessageID == mdl_sys_msg::SubscribeResponse::MessageID) {
            const mdl_sys_msg::SubscribeResponse* reply = (const mdl_sys_msg::SubscribeResponse*)msg->GetBody();
            printf("SubscribeResponse: ");
            for (uint32_t i = 0; i < reply->Services.Length; ++i) {
                for (uint32_t j = 0; j < reply->Services[i]->Messages.Length; ++j) {
                    printf(" %d.%d.%d(subscribe %s)", 
                        reply->Services[i]->ServiceID, reply->Services[i]->ServiceVersion, reply->Services[i]->Messages[j]->MessageID,
                        reply->Services[i]->Messages[j]->MessageStatus == MDLEC_OK ? "ok" : "fail");
                }
            }
            printf("\n");
        }
    }

    // print shanghai level1 message
    virtual void OnMDLSHL1Message(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_shl1_msg::Equity2::MessageID) {
            mdl_shl1_msg::Equity2* body = (mdl_shl1_msg::Equity2*)msg->GetBody(); 
            PrintNewStockMessage(body);
        }
        else if (head->MessageID == mdl_shl1_msg::Indexes::MessageID) {
            mdl_shl1_msg::Indexes* body = (mdl_shl1_msg::Indexes*)msg->GetBody(); 
            PrintIndexMessage(body); 
        }
        else if (head->MessageID == mdl_shl1_msg::Bond2::MessageID) {
            mdl_shl1_msg::Bond2* body = (mdl_shl1_msg::Bond2*)msg->GetBody(); 
            PrintNewStockMessage(body);
        }
        else if (head->MessageID == mdl_shl1_msg::Fund2::MessageID) {
            mdl_shl1_msg::Fund2* body = (mdl_shl1_msg::Fund2*)msg->GetBody(); 
            PrintNewStockMessage(body);
        }
    }

    // print shenzhen level1 message
    virtual void OnMDLSZL1Message(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_szl1_msg::SZL1Index::MessageID) {
            mdl_szl1_msg::SZL1Index* body = (mdl_szl1_msg::SZL1Index*)msg->GetBody();
            PrintIndexMessage(body); 
        }
        else if (head->MessageID == mdl_szl1_msg::SZL1Stock::MessageID) {
            mdl_szl1_msg::SZL1Stock* body = (mdl_szl1_msg::SZL1Stock*)msg->GetBody();
            PrintStockMessage(body); 
        }
    }

    void OnMDLSHL2Message(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_shl2_msg::SHL2MarketData::MessageID) {
            mdl_shl2_msg::SHL2MarketData* body = (mdl_shl2_msg::SHL2MarketData*)msg->GetBody();
            PRINT_WITH_HEAD(head, "secid=%s\n", body->SecurityID.std_str().c_str());
        }
    }

    void OnMDLSZL2Message(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_szl2_msg::Snapshot300111_v2::MessageID) {
            mdl_szl2_msg::Snapshot300111_v2* body = (mdl_szl2_msg::Snapshot300111_v2*)msg->GetBody();
            PRINT_WITH_HEAD(head, "secid=%s\n", body->SecurityID.std_str().c_str());
        }
        if (head->MessageID == mdl_szl2_msg::CashAuctionParam::MessageID) {
            auto body = (const mdl_szl2_msg::CashAuctionParam*)msg->GetBody();
            PRINT_WITH_HEAD(head, "secid=%s\n", body->SecurityID.std_str().c_str());
        }
    }

    void OnMDLSGEMessage(const MDLMessage* msg) {
        MDLMessageHead* head = msg->GetHead();
        if (head->MessageID == mdl_sge_msg::GoldPrice::MessageID) {
            auto body = (mdl_sge_msg::GoldPrice*)msg->GetBody();
            PRINT_WITH_HEAD(head, "secid=%s\n", body->InstruID.std_str().c_str());
        }
    }
};

bool test_fuzzy_sub(const std::string& server) {
    auto ioman = CreateIOManager(1);
    if (ioman.IsNull()) {
        printf("Incompatible API lib version.\n");
        return false;
    }
    MyMessageHandler handler;
    auto sub = ioman->CreateSubscriber(&handler);
    sub->SetMessageEncoding(MDLEID_MKTPRO);
    sub->SetServerAddress(server.c_str());
    sub->SetUserName("test"); // Replace with your TOKEN!

    {
        // const char* sec_ids[] = {"6*", "11*"};
        // sub->SubcribeMessageByFieldValues<mdl_shl2_msg::SHL2MarketData>("SecurityID", sec_ids, 2);
        const char* sec_ids[] = {"6*"};
        sub->SubcribeMessageByFieldValues<mdl_shl2_msg::SHL2MarketData>(
            "SecurityID", sec_ids, sizeof(sec_ids)/sizeof(sec_ids[0]));
    }
    {
        const char* sec_ids[] = {"00*"};
        sub->SubcribeMessageByFieldValues<mdl_szl2_msg::Snapshot300111_v2>(
            "SecurityID", sec_ids, sizeof(sec_ids)/sizeof(sec_ids[0]));
    }

    // sub->SubcribeMessage<mdl_sge_msg::GoldPrice>();
    // sub->SubcribeMessage<mdl_szl2_msg::CashAuctionParam>();

    std::string err = sub->Connect();
    if (!err.empty()) {
        printf("Connect to server failed: %s.\n", err.c_str());
        ioman->Shutdown();
        ioman.Reset();
        return false;
    }
    printf("Connect to server successfully, subscripitions:%s\n", sub->GetSubscription());

    printf("Press enter to exit.\n");
    getchar();
    ioman->Shutdown();
    ioman.Reset();
    return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[]) { 
    if (argc != 2) {
        printf("usage: %s server_hostname:port\n", argv[0]);
        return 1;
    }
    // return test_fuzzy_sub(argv[1]);

    IOManagerPtr m_IOManager = CreateIOManager(1);
    if (m_IOManager.IsNull()) {
        printf("Incompatible API lib version.\n");
        return 1;
    }

    MyMessageHandler handler;
    SubscriberPtr sub = m_IOManager->CreateSubscriber(&handler);

    const char*  securityIDs[]  = {"600000", "600010", "600020"};
    sub->SubcribeMessageByFieldValues<mdl_shl1_msg::Equity2>("SecurityID", securityIDs, 3);
    sub->SubcribeMessage<mdl_shl1_msg::Indexes>();
    sub->SubcribeMessage<mdl_shl1_msg::Bond2>();
    sub->SubcribeMessage<mdl_shl1_msg::Fund2>();
    sub->SetServerAddress(argv[1]);
    sub->SetMessageEncoding(MDLEID_MKTPRO);
    std::string err = sub->Connect();
    if (!err.empty()) {
        printf("Connect to server failed: %s.\n", err.c_str());
        m_IOManager->Shutdown();
        m_IOManager.Reset();
        return -1;
    }

    printf("Connect to server successfully, subscripitions:%s\n", sub->GetSubscription());

    std::this_thread::sleep_for(std::chrono::seconds(10));

    const char*  delIDs[]  = {"600010", "600020"};
    sub->UnSubcribeMessageByFieldValues<mdl_shl1_msg::Equity2>("SecurityID", delIDs, 2);
    sub->UnSubcribeMessage<mdl_shl1_msg::Indexes>();
    sub->UnSubcribeMessage<mdl_shl1_msg::Bond2>();
    sub->UnSubcribeMessage<mdl_shl1_msg::Fund2>();
    sub->ReSubscribe();
    printf("UnsubcribeMesage shl1_Equity(600010, 600020), subscripitions:%s\n", sub->GetSubscription());
    std::this_thread::sleep_for(std::chrono::seconds(10));

    sub->ClearSubscriptions();
    sub->ReSubscribe();
    printf("Delete all subscripitions, subscripitions:%s\n", sub->GetSubscription());
    std::this_thread::sleep_for(std::chrono::seconds(10));

    const char*  securityIDs1[]  = {"600010"};
    sub->SubcribeMessageByFieldValues<mdl_shl1_msg::Equity2>("SecurityID", securityIDs1, 1);
    sub->SubcribeMessage<mdl_shl1_msg::Fund2>();
    sub->ReSubscribe();
    printf("resubscibe message, shl1_equity(600010), shl1_index, subscripitions:%s\n", sub->GetSubscription());

    printf("Press enter to exit.\n");
    getchar();
    m_IOManager->Shutdown();
    m_IOManager.Reset();

    return 0;
}

