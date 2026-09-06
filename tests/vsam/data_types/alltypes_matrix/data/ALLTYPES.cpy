      *================================================================*
      * COPYBOOK  : ALLTYPES                                           *
      * DATABASE  : IMSDEV      (HDAM)                                 *
      * SEGMENT   : ALLTYPES    (child of ROOT)                        *
      * SOURCE    : IMSDEV DBD.xml                                     *
      *----------------------------------------------------------------*
      * Datatype mapping used:                                         *
      *   INT/LONG/SHORT (no typeConverter)      -> signed  COMP        *
      *   UINT/ULONG/USHORT (no typeConverter)    -> unsigned COMP       *
      *   FLOAT                                   -> COMP-1             *
      *   DOUBLE                                  -> COMP-2             *
      *   DECIMAL + PACKEDDECIMAL                 -> COMP-3 (packed)    *
      *   DECIMAL + ZONEDDECIMAL                  -> DISPLAY (zoned)    *
      *   CHAR                                    -> PIC X              *
      * ALL_ID redefines ID (identical position/length in the DBD).     *
      *================================================================*
       01  ALLTYPES-SEGMENT.
      *    --- ID / ALL_ID (key)               bytes  1- 4 -----------*
           05  ALLTYPES-ID                     PIC S9(9)      COMP.
           05  ALLTYPES-ALL-ID REDEFINES ALLTYPES-ID
                                               PIC S9(9)      COMP.
      *    --- INT4                            bytes  5- 8 -----------*
           05  ALLTYPES-INT4                   PIC S9(9)      COMP.
      *    --- UINT4                           bytes  9-12 -----------*
           05  ALLTYPES-UINT4                  PIC 9(9)       COMP.
      *    --- LONG8                           bytes 13-20 -----------*
           05  ALLTYPES-LONG8                  PIC S9(18)     COMP.
      *    --- ULONG8                          bytes 21-28 -----------*
           05  ALLTYPES-ULONG8                 PIC 9(18)      COMP.
      *    --- SHORT2                          bytes 29-30 -----------*
           05  ALLTYPES-SHORT2                 PIC S9(4)      COMP.
      *    --- USHORT2                         bytes 31-32 -----------*
           05  ALLTYPES-USHORT2                PIC 9(4)       COMP.
      *    --- FLOAT4  (single precision fp)   bytes 33-36 -----------*
           05  ALLTYPES-FLOAT4                 USAGE COMP-1.
      *    --- DOUBLE8 (double precision fp)   bytes 37-44 -----------*
           05  ALLTYPES-DOUBLE8                USAGE COMP-2.
      *    --- DECIMAL5V2 (packed, signed)     bytes 45-48 -----------*
           05  ALLTYPES-DECIMAL5V2             PIC S9(5)V9(2) COMP-3.
      *    --- NUMERIC5V2 (zoned, signed)      bytes 49-55 -----------*
           05  ALLTYPES-NUMERIC5V2             PIC S9(5)V9(2) DISPLAY.
      *    --- UNSIGNEDNUM5V2 (zoned, unsigned) bytes 56-62 ----------*
           05  ALLTYPES-UNSIGNEDNUM5V2         PIC 9(5)V9(2)  DISPLAY.
      *    --- NAME                            bytes 63-82 -----------*
           05  ALLTYPES-NAME                   PIC X(20).
      *    --- PRIORITY_ENUM                   bytes 83-84 -----------*
           05  ALLTYPES-PRIORITY-ENUM          PIC X(2).
      *
      *    Fixed segment length ......... 84 bytes
