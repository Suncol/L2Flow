#include "mdl_api_types.h"
#include <string>
#include <vector>


template <typename MDLMessageT>
bool MatchMessage(const datayes::mdl::MDLMessageHead& head) {
    return head.ServiceID == MDLMessageT::ServiceID &&
        head.MessageID == MDLMessageT::MessageID;
}

template <class T>
std::string DoubleToString(const T &mdlDouble) {
    if (mdlDouble.IsNull()) {
        return std::string("null");
    }
    char strBuf[100];
#if defined(__linux__)
    sprintf(strBuf, "%.2f", mdlDouble.GetDouble());
#else
    sprintf_s(strBuf, sizeof(strBuf), "%.2f", mdlDouble.GetDouble());
#endif
    return std::string(strBuf);
}

template <class T>
std::string FloatToString(const T &mdlFloat) {
    if (mdlFloat.IsNull()) {
        return std::string("null");
    }
    char strBuf[100];
#if defined(__linux__)
    sprintf(strBuf, "%.2f", mdlFloat.GetFloat());
#else
    sprintf_s(strBuf, sizeof(strBuf), "%.2f", mdlFloat.GetFloat());
#endif
    return std::string(strBuf);
}

template <class T>
void PrintIndexMessage(const T *msgBody) {
    printf("%d:%02d:%02d %s %s HighIndex:%s LowIndex:%s LastIndex:%s\n",
        msgBody->UpdateTime.GetHour(), msgBody->UpdateTime.GetMinute(),
        msgBody->UpdateTime.GetSecond(), msgBody->IndexID.std_str().c_str(),
        msgBody->IndexName.std_str().c_str(),
        DoubleToString(msgBody->HighIndex).c_str(),
        DoubleToString(msgBody->LowIndex).c_str(),
        DoubleToString(msgBody->LastIndex).c_str());
}
template <class T>
void PrintNewStockMessage(const T *msgBody) {
    printf("%d:%02d:%02d %s %s HighPrice:%s LowPrice:%s LastPrice:%s\n",
        msgBody->UpdateTime.GetHour(), msgBody->UpdateTime.GetMinute(),
        msgBody->UpdateTime.GetSecond(), msgBody->SecurityID.std_str().c_str(),
        msgBody->SecurityName.std_str().c_str(),
        DoubleToString(msgBody->HighPrice).c_str(),
        DoubleToString(msgBody->LowPrice).c_str(),
        DoubleToString(msgBody->LastPrice).c_str());
}
template <class T>
void PrintStockMessage(const T *msgBody) {
    printf("%d:%02d:%02d %s %s HighPrice:%s LowPrice:%s LastPrice:%s\n",
        msgBody->UpdateTime.GetHour(), msgBody->UpdateTime.GetMinute(),
        msgBody->UpdateTime.GetSecond(), msgBody->SecurityID.std_str().c_str(),
        msgBody->SecurityName.std_str().c_str(),
        FloatToString(msgBody->HighPrice).c_str(),
        FloatToString(msgBody->LowPrice).c_str(),
        FloatToString(msgBody->LastPrice).c_str());
}

template <class T>
void PrintFutureMessage(const T *msgBody) {
    printf("%d:%02d:%02d %s HighPrice:%s LowPrice:%s LastPrice:%s\n",
        msgBody->UpdateTime.GetHour(), msgBody->UpdateTime.GetMinute(),
        msgBody->UpdateTime.GetSecond(), msgBody->InstruID.std_str().c_str(),
        DoubleToString(msgBody->HighPrice).c_str(),
        DoubleToString(msgBody->LowPrice).c_str(),
        DoubleToString(msgBody->LastPrice).c_str());
}

typedef std::pair<int, int> MsgId;
struct ServerConfig {
    std::string address;
    std::vector<MsgId> messages;
};

#define PRINT_WITH_HEAD(head, fmt, ...) \
    printf("[%2u.%-2u] " fmt, head->ServiceID, head->MessageID, __VA_ARGS__)
