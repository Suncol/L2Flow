#include <stdio.h>
#include "tools.h"
#include "mdl_api.h"
#include "mdl_shl1_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"
#include "mdl_szl1_msg.h"
#include "mdl_cffex_msg.h"
#include "mdl_czce_msg.h"
#include "mdl_dce_msg.h"
#include "mdl_shfe_msg.h"
#include "mdl_hkex_msg.h"
#include "mdl_bar_msg.h"
#include "mdl_neeq_msg.h"
#ifdef __linux__
#include <unistd.h>
#endif

using namespace datayes::mdl;

class MyMessageHandler : public MessageHandler {
public:
	bool Open(const char* address, const char * token) {
		int num_work_threads = 4;
		m_IOManager = CreateIOManager(num_work_threads);
		if (m_IOManager.IsNull()) {
			printf("Incompatible API lib version.\n");
			return false;
		}

		m_IOManager->EnableLog();
		
		// auto-subscriber queries server to get latest config, then subscribes messages from servers in the config
		// config will be saved to autoconf.json, the file will be used as backup if query failed on next startup

		bool multi_thread_callback = true; // set to true if num_work_threads > 1 and OnXXXMessage() is thread-safe
		m_autosub = m_IOManager->CreateAutoSubscriber(this, multi_thread_callback);
		m_autosub->SetURL(address);
		m_autosub->SetToken(token);

		// Options:
		//   Network: string, network type ("internet" or "d-line")
		//   ExMsgs: string, messages NOT to subscribe, comma delimited
		//   ExSvrs: string, servers NOT to connect, comma delimited
		//   Role: string, type of data ("primary" or "secondary")
		// m_autosub->SetOptionsJson("{\"ExMsgs\": \"3.8,3.9,3.10,5.2\",\"ExSvrs\":\"mdl-cloud-sz.datayes.com\"}");

		// set time to check server config every day, default 520 (05:20)
		//   if server config changes, existing autoconf.json will be renamed to autoconf.json.0
		// m_autosub->SetDailyCheckTime(520); 

		// connect to servers and subscribe messages
		std::string err = m_autosub->Start();
		if (err.empty()) {
			printf("auto-subscriber started successfully.\n");
			return true;
		}
		printf("auto-subscriber start failed: %s\n", err.c_str());
		return false; 
	}
 
	void Close() {
		if (!m_IOManager.IsNull()) {
			m_IOManager->Shutdown();
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

	// handle server response
	virtual void OnMDLSysMessage(const MDLMessage* msg) {
		MDLMessageHead* head = msg->GetHead();
		if (head->MessageID == mdl_sys_msg::LogonResponse::MessageID) {
			mdl_sys_msg::LogonResponse* resp = (mdl_sys_msg::LogonResponse*)msg->GetBody();
			if (resp->ReturnCode != MDLEC_OK) {
				printf("Logon failed: return code %d.\n", resp->ReturnCode);
			}
			for (uint32_t i = 0; i < resp->Services.Length; ++i) {
				for (uint32_t j = 0; j < resp->Services[i]->Messages.Length; ++j) {
					if (resp->Services[i]->Messages[j]->MessageStatus != MDLEC_OK) {
						printf("The server doesn't publish message (service id %d message id %d)\n", 
							resp->Services[i]->ServiceID,
							resp->Services[i]->Messages[j]->MessageID);
					}
				}
			}
		}
	}

	// print shfe future message
	virtual void OnMDLSHFEMessage(const MDLMessage* msg) {
		MDLMessageHead* head = msg->GetHead();
		if (head->MessageID == mdl_shfe_msg::CTPFuture::MessageID) {
			mdl_shfe_msg::CTPFuture* body = (mdl_shfe_msg::CTPFuture*)msg->GetBody();
			PrintFutureMessage(body);
		}
	}

	// print czce future message
	virtual void OnMDLCZCEMessage(const MDLMessage* msg) {
		MDLMessageHead* head = msg->GetHead();
		if (head->MessageID == mdl_czce_msg::CTPFuture::MessageID) {
			mdl_czce_msg::CTPFuture* body = (mdl_czce_msg::CTPFuture*)msg->GetBody();
			PrintFutureMessage(body);
		}
	}

	// print cffex future message
	virtual void OnMDLCFFEXMessage(const MDLMessage* msg) {
		MDLMessageHead* head = msg->GetHead();
		if (head->MessageID == mdl_cffex_msg::CTPFuture::MessageID) {
			mdl_cffex_msg::CTPFuture* body = (mdl_cffex_msg::CTPFuture*)msg->GetBody();
			PrintFutureMessage(body);
		}
	}

	// print dce future message
	virtual void OnMDLDCEMessage(const MDLMessage* msg) {
		MDLMessageHead* head = msg->GetHead();
		if (head->MessageID == mdl_dce_msg::CTPFuture::MessageID) {
			mdl_dce_msg::CTPFuture* body = (mdl_dce_msg::CTPFuture*)msg->GetBody();
			PrintFutureMessage(body);
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
		if (head->MessageID == mdl_szl1_msg::Index2::MessageID) {
			mdl_szl1_msg::Index2* body = (mdl_szl1_msg::Index2*)msg->GetBody();
			PrintIndexMessage(body); 
		}
		else if (head->MessageID == mdl_szl1_msg::SZL1Stock::MessageID) {
			mdl_szl1_msg::SZL1Stock* body = (mdl_szl1_msg::SZL1Stock*)msg->GetBody();
			PrintStockMessage(body); 
		}
	}

	// print hkex message
	virtual void OnMDLHKExMessage(const MDLMessage* msg) {
		MDLMessageHead* head = msg->GetHead();
		if (head->MessageID == mdl_hkex_msg::OMDMarketData::MessageID) {
			mdl_hkex_msg::OMDMarketData* body = (mdl_hkex_msg::OMDMarketData*)msg->GetBody();
			printf("[%d:%02d:%02d.%d] %s ",
				body->TradTime.GetHour(), body->TradTime.GetMinute(), body->TradTime.GetSecond(), body->TradTime.GetMilliSec(),
				body->TickerSymbol.c_str());
			if (body->LastPrice.IsNull()) {
				printf("LastPrice:null");
			}
			else {
				printf("LastPrice:%.2f", body->LastPrice.GetFloat());
			}	
			if (body->ChangePct.IsNull()) {
				printf("(),");
			}
			else {
				printf("(%.3f%%),", body->ChangePct.GetFloat() * 100);
			}	
			printf("Volume:%.3fM,", (float)body->Quantity / 1000000.0);
			if (body->Turnover.IsNull()) {
				printf("Turnover:null\n");
			}
			else {
				printf("Turnover:%.3fM\n", body->Turnover.GetDouble() / 1000000.0);
			}	

			if (body->AskBook.Length > 0) {
				if (body->AskBook[0]->Price.IsNull()) {
					printf("	Ask1:null,0,");
				}
				else {
					printf("	Ask1:%.3f,%lu,", body->AskBook[0]->Price.GetFloat(), body->AskBook[0]->Volume);
				}
			}
			if (body->BidBook.Length > 0) {
				if (body->BidBook[0]->Price.IsNull()) {
					printf("	Bid1:null,0");
				}
				else {
					printf("	Bid1:%.3f,%lu", body->BidBook[0]->Price.GetFloat(), body->BidBook[0]->Volume);
				}
			} 
			printf("\n");
		}
	}
	// handle shenzhen level2 message
	void OnMDLSZL2Message(const MDLMessage* msg) {
		MDLMessageHead* head = msg->GetHead();
		if (head->MessageID == mdl_szl2_msg::Snapshot300111_v2::MessageID) {
			mdl_szl2_msg::Snapshot300111_v2 * resp = (mdl_szl2_msg::Snapshot300111_v2*)msg->GetBody();
			printf("[%d:%02d:%02d] %s %s Hi:%.6f Lo:%.6f Last:%.6f Vol:%ld Tun:%.4f\n",
				resp->UpdateTime.GetHour(), resp->UpdateTime.GetMinute(), resp->UpdateTime.GetSecond(),
				resp->SecurityID.std_str().c_str(), 
				resp->TradingPhaseCode.c_str(),
				resp->HighPrice.GetDouble(), resp->LowPrice.GetDouble(), resp->LastPrice.GetDouble(),
				resp->Volume,
				resp->Turnover.GetDouble());

			for (size_t i = 0; i < 10; ++i) {
				double askPrice = 0.0;
				int64_t askVolumn = 0.0;
				double bidPrice = 0.0;
				int64_t bidVolumn = 0.0;
				if (i < resp->AskPriceLevel.Length) {
					askPrice = resp->AskPriceLevel[i]->Price.GetDouble();
					askVolumn = resp->AskPriceLevel[i]->Volume;
				}
				if (i < resp->BidPriceLevel.Length) {
					bidPrice = resp->BidPriceLevel[i]->Price.GetDouble();
					bidVolumn = resp->BidPriceLevel[i]->Volume;
				}
				printf("%.6f %ld\t\t%.6f %ld\n", askPrice, askVolumn, bidPrice, bidVolumn);
			}
		}
		else if (head->MessageID == mdl_szl2_msg::Order300192_v2::MessageID) {
			mdl_szl2_msg::Order300192_v2 * resp = (mdl_szl2_msg::Order300192_v2*)msg->GetBody();
			printf("[%d:%02d:%02d] %s Side:%d Price:%.4f Qty:%ld\n",
				resp->TransactTime.GetHour(), resp->TransactTime.GetMinute(), resp->TransactTime.GetSecond(),
				resp->SecurityID.std_str().c_str(), 
				resp->Side,
				resp->Price.GetDouble(), resp->OrderQty);
		}
		else if (head->MessageID == mdl_szl2_msg::Transaction300191_v2::MessageID) {
			mdl_szl2_msg::Transaction300191_v2 * resp = (mdl_szl2_msg::Transaction300191_v2*)msg->GetBody();
			printf("[%d:%02d:%02d] %s ET:%d Price:%.4f Qty:%ld\n",
				resp->TransactTime.GetHour(), resp->TransactTime.GetMinute(), resp->TransactTime.GetSecond(),
				resp->SecurityID.std_str().c_str(), 
				resp->ExecType,
				resp->LastPx.GetDouble(), resp->LastQty);
		}
	}

	void OnMDLSHL2Message(const MDLMessage* msg) {
		if (msg->GetHead()->MessageID == mdl_shl2_msg::SHL2Index::MessageID) {
			mdl_shl2_msg::SHL2Index * resp = (mdl_shl2_msg::SHL2Index*)msg->GetBody();
			printf("#%lu [%d:%02d:%02d] %s Hi:%.2f Lo:%.2f Last:%.2f\n",
				msg->GetHead()->SequenceID,
				resp->UpdateTime.GetHour(), resp->UpdateTime.GetMinute(), resp->UpdateTime.GetSecond(),
				resp->SecurityID.std_str().c_str(),  
				resp->HighIndex.GetDouble(), resp->LowIndex.GetDouble(), resp->LastIndex.GetDouble());
		}	
		else if (msg->GetHead()->MessageID == mdl_shl2_msg::SHL2MarketData::MessageID) {
			mdl_shl2_msg::SHL2MarketData * resp = (mdl_shl2_msg::SHL2MarketData*)msg->GetBody();
 			printf("#%lu [%d:%02d:%02d] %s Hi:%.2f Lo:%.2f Last:%.2f\n",
				msg->GetHead()->SequenceID,
				resp->UpdateTime.GetHour(), resp->UpdateTime.GetMinute(), resp->UpdateTime.GetSecond(),
				resp->SecurityID.std_str().c_str(), 
				resp->HighPrice.GetFloat(),	resp->LowPrice.GetFloat(), resp->LastPrice.GetFloat());
		}
		else if (msg->GetHead()->MessageID == mdl_shl2_msg::SHL2MarketOverview::MessageID) {
			mdl_shl2_msg::SHL2MarketOverview * resp = (mdl_shl2_msg::SHL2MarketOverview*)msg->GetBody();
		}
		else if (msg->GetHead()->MessageID == mdl_shl2_msg::SHL2Transaction::MessageID) {
			mdl_shl2_msg::SHL2Transaction * resp = (mdl_shl2_msg::SHL2Transaction*)msg->GetBody();
		}
		else if (msg->GetHead()->MessageID == mdl_shl2_msg::SHL2VirtualAuctionPrice::MessageID) {
			mdl_shl2_msg::SHL2VirtualAuctionPrice * resp = (mdl_shl2_msg::SHL2VirtualAuctionPrice*)msg->GetBody();
		}
		else if (msg->GetHead()->MessageID == mdl_shl2_msg::OPTLevel1::MessageID) {
			mdl_shl2_msg::OPTLevel1 * resp = (mdl_shl2_msg::OPTLevel1*)msg->GetBody();
 			printf("#%lu [%d:%02d:%02d] %s Hi:%.2f Lo:%.2f Last:%.2f\n",
				msg->GetHead()->SequenceID,
				resp->UpdateTime.GetHour(), resp->UpdateTime.GetMinute(), resp->UpdateTime.GetSecond(),
				resp->SecurityID.std_str().c_str(), 
				resp->HighPx.GetDouble(),	resp->LowPx.GetDouble(), resp->LastPx.GetDouble());
		}
	}

	virtual void OnMDLBARMessage(const MDLMessage* msg) {
		if (msg->GetHead()->MessageID == mdl_bar_msg::XSHGStockMinuteBar::MessageID) {
			mdl_bar_msg::XSHGStockMinuteBar * resp = (mdl_bar_msg::XSHGStockMinuteBar*)msg->GetBody();
 			printf("#%lu [%d:%02d:%02d] %s Hi:%.2f Lo:%.2f Last:%.2f\n",
				msg->GetHead()->SequenceID,
				resp->BarTime.GetHour(), resp->BarTime.GetMinute(), resp->BarTime.GetSecond(),
				resp->SecurityID.std_str().c_str(), 
				resp->HighPrice.GetDouble(), resp->LowPrice.GetDouble(), resp->LowPrice.GetDouble());
		}
		else if (msg->GetHead()->MessageID == mdl_bar_msg::XSHGCapitalFlow::MessageID) {
			mdl_bar_msg::XSHGCapitalFlow * resp = (mdl_bar_msg::XSHGCapitalFlow*)msg->GetBody();
 			printf("#%lu [%d:%02d:%02d] %s %s Px:%.2f Vol:%ld NetIn:%.2f\n",
				msg->GetHead()->SequenceID,
				resp->TradeTime.GetHour(), resp->TradeTime.GetMinute(), resp->TradeTime.GetSecond(),
				resp->SecurityID.std_str().c_str(), 
				resp->BSFlag == 1 ? "B" : (resp->BSFlag == 2 ? "S" : "NA"),
				resp->Price.GetDouble(), resp->Volume, resp->NetCapitalInflow.GetDouble());
		}
		else if (msg->GetHead()->MessageID == mdl_bar_msg::IndustryCapitalFlow::MessageID) {
			mdl_bar_msg::IndustryCapitalFlow * resp = (mdl_bar_msg::IndustryCapitalFlow*)msg->GetBody();
 			printf("#%lu [%d:%02d:%02d] %s %s In:%.2f OutIn:%.2f NetIn:%.2f\n",
				msg->GetHead()->SequenceID,
				resp->TradeTime.GetHour(), resp->TradeTime.GetMinute(), resp->TradeTime.GetSecond(),
				resp->SecurityID.std_str().c_str(), 
				resp->SecurityName.std_str().c_str(),  
				resp->CapitalInFlow.GetDouble(),
				resp->CapitalOutFlow.GetDouble(),
				resp->NetCapitalInflow.GetDouble());
		}
	}

	virtual void OnMDLNEEQMessage(const MDLMessage* msg) {
		// NEEQ messages are not supported in this demo
		MDLMessageHead *head = msg->GetHead();
		if (head->MessageID == mdl_neeq_msg::BJStock::MessageID) {
			const mdl_neeq_msg::BJStock *resp = (mdl_neeq_msg::BJStock *) msg->GetBody();
			PrintStockMessage(resp);
		}
	}

private:
	IOManagerPtr m_IOManager;
	AutoSubscriberPtr m_autosub;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[]) { 
	if (argc != 2) {
		printf("usage: %s <Token>\n", argv[0]);
		return 1;
	}

	MyMessageHandler msgHandler;
	if (msgHandler.Open("https://mdl01.datayes.com:19000/subscribe", argv[1])) {
		printf("Receiving message, press enter to stop.\n");
		getchar();
	}

	msgHandler.Close(); 
	printf("Press enter to exit.\n");
	getchar();
	return 0;
}

