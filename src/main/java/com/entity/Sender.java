package com.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @Name Sender
 * @Author shenYue
 * @Data 2022/1/14 16:52
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sender {
    private String name;
    private Integer number;
    private String time;
}
