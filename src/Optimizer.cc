#include "../include/Optimizer.h"

Optimizer::Optimizer() {
    currentState = new StatisticsState();
}

void Optimizer::ReadParserDatastructures() {
    std::cout<<"Here."<<std::endl;
}

void Optimizer::Read(char *fromWhere) {
    currentState->Read(fromWhere);
}