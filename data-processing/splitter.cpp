/*
 * Copyright (c) Computer Systems Research Group @ PKU (xingyu xiang)

 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>

int NumOfChunks;
std::string DataDir = "../../data/";
const std::string LogDir = "log/";
std::string FileToSplit;

int main(int argc, char *argv[]) {
    FileToSplit = std::string(argv[1]);
    NumOfChunks = std::stoi(std::string(argv[2]));

    std::string Buffer;
    ssize_t NumOfLines;

    std::string Command = "wc -l " + DataDir + FileToSplit + ".dat > " + LogDir + FileToSplit + "_lines";
    system (Command.c_str());
    std::ifstream LineFile(LogDir + FileToSplit + "_lines");
    LineFile >> NumOfLines;
    LineFile.close();

    std::ifstream Data(DataDir + FileToSplit + ".dat");
    std::ofstream Log(LogDir + FileToSplit + "_log");

    for (int i = 0; i < NumOfChunks; ++i) {
        Log << "Chunk " << i << std::endl;
        std::ofstream Chunk(DataDir + FileToSplit + "_" + std::to_string(i) + ".dat");
        ssize_t NumOfChunkLines = (i == NumOfChunks - 1) ? ((NumOfLines % NumOfChunks) + (NumOfLines / NumOfChunks)) : NumOfLines / NumOfChunks;
        for (int j = 0; j < NumOfChunkLines; ++j) {
            getline(Data, Buffer);
            Chunk << Buffer << std::endl;
        }
        Chunk.close();
    }

    Data.close();
    Log.close();

    return 0;
}