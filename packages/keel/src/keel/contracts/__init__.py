"""Protocols every subsystem is written against.

Contracts live in their own package so that a driver can import the interface it
implements without importing the implementations it sits alongside. It also
keeps the answer to "what does a store have to do?" in one file, rather than
spread across the drivers that happen to do it.
"""
